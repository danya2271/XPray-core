//! Interoperability client, intentionally separate from the production backend.
//! Uses adaptive Quinn congestion control; Brutal is not wired into Quinn yet.

use std::{
    collections::HashMap,
    env,
    error::Error,
    fs::File,
    future::poll_fn,
    io::BufReader,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use quinn::{AsyncUdpSocket, Runtime};
use xhysteria::{
    protocol,
    socket::MaskedSocket,
    udp::{Defragger, Message},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const TIMEOUT: Duration = Duration::from_secs(10);

fn options() -> Result<HashMap<String, String>> {
    let mut args = env::args().skip(1);
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if ![
            "--server",
            "--server-name",
            "--ca",
            "--auth-env",
            "--salamander-env",
            "--tcp",
            "--udp",
            "--payload-size",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unknown option: {key}").into());
        }
        let value = args.next().ok_or("missing option value")?;
        if options.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    Ok(options)
}

fn required<'a>(options: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("missing {key}").into())
}

async fn tcp_probe(connection: &quinn::Connection, target: &str, payload: &[u8]) -> Result<()> {
    let (mut tx, mut rx) = connection.open_bi().await?;
    let mut request = protocol::encode_tcp_request(target.as_bytes(), b"native-interop-padding")?;
    request.extend_from_slice(payload);
    tx.write_all(&request).await?;
    tx.finish()?;
    let response = rx
        .read_to_end(payload.len() + protocol::MAX_MESSAGE + protocol::MAX_PADDING + 32)
        .await?;
    let header = protocol::parse_tcp_response(&response)?;
    if !header.accepted {
        return Err("TCP proxy request rejected".into());
    }
    if response[header.consumed..] != *payload {
        return Err("TCP echo payload mismatch".into());
    }
    println!("tcp_ok bytes={}", payload.len());
    Ok(())
}

async fn udp_probe(connection: &quinn::Connection, target: &str, payload: &[u8]) -> Result<()> {
    let message = Message {
        session: 1,
        packet: 17,
        fragment: 0,
        count: 1,
        address: target.as_bytes(),
        payload,
    };
    let limit = connection
        .max_datagram_size()
        .ok_or("peer has no QUIC DATAGRAM support")?;
    let fragments = message.fragment(limit)?;
    let mut wire = Vec::new();
    for part in &fragments {
        part.encode(&mut wire)?;
        connection
            .send_datagram_wait(Bytes::copy_from_slice(&wire))
            .await?;
    }
    let mut defrag = Defragger::new(8)?;
    let start = Instant::now();
    loop {
        let packet = connection.read_datagram().await?;
        let message = Message::decode(&packet)?;
        if let Some(datagram) = defrag.feed(start.elapsed().as_nanos() as u64, &message)? {
            if datagram.session != 1
                || datagram.address != target.as_bytes()
                || datagram.payload != payload
            {
                return Err("UDP echo payload/address mismatch".into());
            }
            println!(
                "udp_ok bytes={} sent_fragments={}",
                payload.len(),
                fragments.len()
            );
            return Ok(());
        }
    }
}

async fn run() -> Result<()> {
    let options = options()?;
    let server: SocketAddr = required(&options, "--server")?.parse()?;
    let server_name = required(&options, "--server-name")?;
    let auth = env::var(required(&options, "--auth-env")?)
        .map_err(|_| "authentication environment variable missing")?;
    let payload_size = options
        .get("--payload-size")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(4096);
    if payload_size == 0 || payload_size > xhysteria::udp::MAX_PAYLOAD {
        return Err("payload size must be between 1 and 4096".into());
    }
    let mut roots = rustls::RootCertStore::empty();
    let mut pem = BufReader::new(File::open(required(&options, "--ca")?)?);
    for certificate in rustls_pemfile::certs(&mut pem) {
        roots.add(certificate?)?;
    }
    if roots.is_empty() {
        return Err("CA file contains no certificates".into());
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(crypto));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(TIMEOUT.try_into()?));
    transport.keep_alive_interval(Some(Duration::from_secs(2)));
    transport.datagram_receive_buffer_size(Some(256 * 1024));
    transport.datagram_send_buffer_size(256 * 1024);
    // Keep a conservative outer UDP MTU after adding the eight-byte salt.
    if options.contains_key("--salamander-env") {
        transport
            .initial_mtu(1200)
            .min_mtu(1200)
            .mtu_discovery_config(None);
    }
    client_config.transport_config(Arc::new(transport));
    let local = if server.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(local)?;
    socket.set_nonblocking(true)?;
    let runtime = Arc::new(quinn::TokioRuntime);
    let udp_socket: Arc<dyn AsyncUdpSocket> =
        if let Some(variable) = options.get("--salamander-env") {
            let psk = env::var(variable).map_err(|_| "Salamander environment variable missing")?;
            Arc::new(MaskedSocket::new(socket, psk.as_bytes())?)
        } else {
            runtime.wrap_udp_socket(socket)?
        };
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        udp_socket,
        runtime,
    )?;
    endpoint.set_default_client_config(client_config);
    let connection = endpoint.connect(server, server_name)?.await?;
    let (mut h3_driver, mut requests) =
        h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
    let driver = tokio::spawn(async move { poll_fn(|cx| h3_driver.poll_close(cx)).await });
    let result: Result<()> = async {
        let request = http::Request::builder()
            .method("POST")
            .uri("https://hysteria/auth")
            .header("Hysteria-Auth", auth)
            .header("Hysteria-CC-RX", "0")
            .header("Hysteria-Padding", "n".repeat(256))
            .body(())?;
        let mut stream = requests.send_request(request).await?;
        stream.finish().await?;
        let response = stream.recv_response().await?;
        if response.status().as_u16() != 233 {
            return Err(
                format!("authentication failed: HTTP {}", response.status().as_u16()).into(),
            );
        }
        let udp_enabled = response
            .headers()
            .get("Hysteria-UDP")
            .is_some_and(|v| v == "true");
        while stream.recv_data().await?.is_some() {}
        println!("auth_ok udp={udp_enabled}");
        let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();
        if let Some(target) = options.get("--tcp") {
            tcp_probe(&connection, target, &payload).await?;
        }
        if let Some(target) = options.get("--udp") {
            if !udp_enabled {
                return Err("peer disabled UDP relay".into());
            }
            udp_probe(&connection, target, &payload).await?;
        }
        Ok(())
    }
    .await;
    connection.close(0x100u32.into(), b"probe complete");
    driver.abort();
    let _ = driver.await;
    endpoint.wait_idle().await;
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match tokio::time::timeout(Duration::from_secs(30), run()).await {
        Ok(Ok(())) => (),
        result => {
            match result {
                Ok(Err(error)) => eprintln!("xhys-probe: {error}"),
                Err(_) => eprintln!("xhys-probe: operation timed out"),
                _ => unreachable!(),
            }
            std::process::exit(1);
        }
    }
}
