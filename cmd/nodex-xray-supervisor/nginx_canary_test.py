#!/usr/bin/env python3
"""Exercise a managed VXT/white upgrade through NodeX-generated Nginx config.

Usage: nginx_canary_test.py XRAY SUPERVISOR VLESS_XHTTP_TLS_FIXTURE [--http2-upstream]
The fixture is DanVlessBot's vless_xhttp_tls_test --streaming-fixture entrypoint.
"""

import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def receive(connection, count):
    result = bytearray()
    while len(result) < count:
        chunk = connection.recv(count - len(result))
        if not chunk:
            raise AssertionError("Tunnel closed before the reply was complete")
        result.extend(chunk)
    return bytes(result)


def open_tunnel(socks, target):
    connection = socket.create_connection(("127.0.0.1", socks), timeout=5)
    try:
        connection.sendall(b"\x05\x01\x00")
        assert receive(connection, 2) == b"\x05\x00"
        connection.sendall(b"\x05\x01\x00\x01\x7f\x00\x00\x01" + struct.pack("!H", target))
        response = receive(connection, 4)
        assert response[:2] == b"\x05\x00", response
        if response[3] == 1:
            receive(connection, 6)
        elif response[3] == 4:
            receive(connection, 18)
        elif response[3] == 3:
            receive(connection, receive(connection, 1)[0] + 2)
        else:
            raise AssertionError(f"Invalid SOCKS response: {response}")
        return connection
    except BaseException:
        connection.close()
        raise


def pulse(connection, marker, value):
    connection.sendall(value)
    assert receive(connection, 2) == marker + value


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def wait_ready(process, port):
    for _ in range(200):
        assert process.poll() is None, f"Process exited with {process.returncode}"
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError(f"Port {port} did not become ready")


@contextlib.contextmanager
def client(xray, template, directory, label):
    config = json.loads(Path(template).read_text())
    port = free_port()
    config["inbounds"][0]["port"] = port
    # Independent transport, native TLS, and the generated CA verification.
    # A new probe must not reuse an old generation's multiplexed connection.
    config["outbounds"][0]["streamSettings"]["tlsSettings"]["fingerprint"] = "unsafe"
    path = directory / f"{label}.json"
    path.write_text(json.dumps(config))
    with open(directory / f"{label}.log", "wb") as log:
        process = subprocess.Popen([xray, "run", "-config", str(path)], stdout=log, stderr=log)
        try:
            wait_ready(process, port)
            yield port
        finally:
            stop(process)


def probe():
    meta = json.loads(Path(os.environ["NODEX_NGINX_PROBE"]).read_text())
    marker = {"g1": b"A", "g2": b"B"}[sys.argv[2]]
    assert len(os.environ["NODEX_CANDIDATE_SHA256"]) == 64
    assert int(os.environ["NODEX_CANDIDATE_PID"]) > 1
    directory = Path(tempfile.mkdtemp(prefix="probe-", dir=meta["directory"]))
    try:
        for profile in ("direct", "white"):
            with client(meta["xray"], meta[profile], directory, profile) as socks, \
                    open_tunnel(socks, meta["target"]) as connection:
                pulse(connection, marker, b"p")
    finally:
        shutil.rmtree(directory)


def main():
    if len(sys.argv) < 4:
        raise SystemExit(__doc__)
    if not shutil.which("nginx") or not shutil.which("openssl"):
        print("SKIP: nginx and openssl are required")
        return 77
    if os.geteuid() == 0:
        print("SKIP: run under an unprivileged canary account")
        return 77
    xray, supervisor, fixture = map(lambda p: str(Path(p).resolve()), sys.argv[1:4])
    upstream = "http2" if "--http2-upstream" in sys.argv[4:] else "http1"
    servers, processes, threads = [], [], []
    with tempfile.TemporaryDirectory(prefix="nx-nginx-") as temporary, contextlib.ExitStack() as stack:
        directory = Path(temporary)

        class Backend(socketserver.ThreadingTCPServer):
            daemon_threads = True

        class Echo(socketserver.BaseRequestHandler):
            def handle(self):
                while data := self.request.recv(1):
                    self.request.sendall(self.server.marker + data)

        def launch(command, label, **kwargs):
            log = stack.enter_context(open(directory / f"{label}.log", "wb"))
            process = subprocess.Popen(command, stdout=log, stderr=log, **kwargs)
            processes.append(process)
            return process

        try:
            for marker in (b"A", b"B"):
                server = Backend(("127.0.0.1", 0), Echo)
                server.marker = marker
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                servers.append(server)
                threads.append(thread)
                thread.start()
            target = servers[0].server_address[1]
            http_port, direct, white = free_port(), free_port(), free_port()
            assert len({http_port, direct, white, target, servers[1].server_address[1]}) == 5
            subprocess.run([fixture, "--streaming-fixture", temporary, str(direct), str(white), upstream, "/vxt/"],
                           check=True, timeout=10)
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                            "-keyout", str(directory / "key.pem"), "-out", str(directory / "cert.pem"),
                            "-days", "1", "-subj", "/CN=origin.test",
                            "-addext", "subjectAltName=DNS:origin.test"],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=10)
            site = (directory / "site.conf").read_text()
            assert ("grpc_pass" in site) == (upstream == "http2")
            for before, after in [
                ("listen 80;", f"listen 127.0.0.1:{http_port};"),
                (f"listen {direct} ssl", f"listen 127.0.0.1:{direct} ssl"),
                (f"listen {white} ssl", f"listen 127.0.0.1:{white} ssl"),
                (f"/dev/shm/xrxh-{direct}.socket", str(directory / "direct.sock")),
                (f"/dev/shm/xrxh-{white}.socket", str(directory / "white.sock")),
                ("/etc/letsencrypt/live/origin.test/fullchain.pem", str(directory / "cert.pem")),
                ("/etc/letsencrypt/live/origin.test/privkey.pem", str(directory / "key.pem")),
            ]:
                site = site.replace(before, after)
            nginx_config = directory / "nginx.conf"
            nginx_config.write_text(
                f"worker_processes 1;\npid {directory}/nginx.pid;\nerror_log {directory}/nginx-error.log info;\n"
                "events {worker_connections 128;}\nhttp {\naccess_log off;\n"
                + "".join(f"{module}_temp_path {directory}/{module};\n"
                          for module in ("client_body", "proxy", "fastcgi", "uwsgi", "scgi"))
                + site + "\n}\n")
            subprocess.run(["nginx", "-t", "-p", temporary, "-c", str(nginx_config)],
                           check=True, capture_output=True, timeout=5)
            configs = []
            for marker, backend in zip(("A", "B"), servers):
                config = json.loads((directory / "server.json").read_text())
                config["outbounds"][0]["settings"] = {
                    "redirect": f"127.0.0.1:{backend.server_address[1]}",
                    "finalRules": [{"action": "allow", "ip": ["127.0.0.1"], "port": port}
                                   for port in {target, backend.server_address[1]}],
                }
                path = directory / f"generation-{marker}.json"
                path.write_text(json.dumps(config))
                configs.append(path)
            nginx = launch(["nginx", "-p", temporary, "-c", str(nginx_config), "-g", "daemon off;"], "nginx")
            wait_ready(nginx, direct)
            wait_ready(nginx, white)
            meta = directory / "probe.json"
            meta.write_text(json.dumps({
                "xray": xray, "directory": temporary, "target": target,
                "direct": str(directory / "direct-client.json"), "white": str(directory / "white-client.json"),
            }))
            env = dict(os.environ, NODEX_NGINX_PROBE=str(meta))
            verify = directory / "verify"
            # Paths are supplied through environment, not shell interpolation.
            verify.write_text('#!/bin/sh\nexec "$NODEX_PYTHON" "$NODEX_NGINX_SCRIPT" --probe "$1"\n')
            verify.chmod(0o700)
            env.update(NODEX_PYTHON=sys.executable, NODEX_NGINX_SCRIPT=str(Path(__file__).resolve()))
            runtime = directory / "run"
            manager = launch([supervisor, "-runtime", str(runtime), "-binary", xray, "-config", str(configs[0]),
                              "-probe", str(verify), "serve"], "supervisor", env=env)

            def status():
                assert manager.poll() is None, f"Supervisor exited with {manager.returncode}"
                result = subprocess.run([supervisor, "-runtime", str(runtime), "status"],
                                        capture_output=True, timeout=5)
                return json.loads(result.stdout) if result.returncode == 0 else None

            def wait(phase, generation):
                for _ in range(300):
                    current = status()
                    if current and current["phase"] == phase and \
                            current["last_confirmed_active"]["id"] == generation:
                        return current
                    if current and current["phase"] in ("blocked", "admission-unknown"):
                        raise AssertionError(current)
                    time.sleep(0.03)
                raise AssertionError(f"Supervisor did not reach {phase}/{generation}: {status()}")

            wait("active", "g1")
            with contextlib.ExitStack() as clients:
                old = []
                for profile in ("direct", "white"):
                    socks = clients.enter_context(client(xray, directory / f"{profile}-client.json", directory, f"old-{profile}"))
                    connection = clients.enter_context(open_tunnel(socks, target))
                    old.append(connection)
                    pulse(connection, b"A", b"0")
                subprocess.run([supervisor, "-runtime", str(runtime), "-binary", xray, "-config", str(configs[1]), "upgrade"],
                               check=True, capture_output=True, timeout=5)
                state = wait("draining", "g2")
                assert state["other"]["id"] == "g1" and state["other"]["running"]
                for connection in old:
                    pulse(connection, b"A", b"1")
                for profile in ("direct", "white"):
                    with client(xray, directory / f"{profile}-client.json", directory, f"new-{profile}") as socks, \
                            open_tunnel(socks, target) as connection:
                        pulse(connection, b"B", b"2")
                for connection in old:
                    pulse(connection, b"A", b"3")
            wait("active", "g2")
            print(f"PASS: NodeX-generated Nginx {upstream}, VLESS stream-up and white packet-up survive managed cutover")
        except BaseException:
            for log in sorted(directory.glob("*.log")):
                print(f"{log.name}:\n{log.read_text(errors='replace')[-2500:]}", file=sys.stderr)
            raise
        finally:
            for process in reversed(processes):
                stop(process)
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=2)
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--probe":
        probe()
    else:
        sys.exit(main())
