//go:build linux && xhysteria_native && xhysteria_interop && cgo

package interop_test

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type handoffSocketStats struct {
	Inode      string
	Drops      uint64
	QueueBytes uint64
}

// Linux exposes sk_drops in the UDP table independently of any relay counter.
// Resolve once by the fixture's exact bind, then require the original inode;
// a socket that disappeared/rebound must not produce a misleading zero.
func parseHandoffSocketStats(reader io.Reader, local, inode string) (handoffSocketStats, error) {
	scanner := bufio.NewScanner(reader)
	var result handoffSocketStats
	found := false
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[1] != local {
			continue
		}
		if len(fields) != 13 {
			return result, errors.New("unsupported UDP socket stats row")
		}
		if inode != "" && fields[9] != inode {
			continue
		}
		if found {
			return result, errors.New("ambiguous UDP socket identity")
		}
		queue := strings.Split(fields[4], ":")
		if len(queue) != 2 {
			return result, errors.New("invalid UDP receive queue")
		}
		drops, err := strconv.ParseUint(fields[12], 10, 64)
		if err != nil {
			return result, err
		}
		queued, err := strconv.ParseUint(queue[1], 16, 64)
		if err != nil {
			return result, err
		}
		if value, err := strconv.ParseUint(fields[9], 10, 64); err != nil || value == 0 {
			return result, errors.New("invalid UDP socket inode")
		}
		result = handoffSocketStats{Inode: fields[9], Drops: drops, QueueBytes: queued}
		found = true
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if !found {
		return result, errors.New("original public UDP socket is not present")
	}
	return result, nil
}

func readHandoffSocketStats(local net.UDPAddr, inode string) (handoffSocketStats, error) {
	ip := local.IP.To4()
	if ip == nil || local.Port < 1 || local.Port > 65535 {
		return handoffSocketStats{}, errors.New("load fixture requires a bound IPv4 socket")
	}
	file, err := os.Open("/proc/self/net/udp")
	if err != nil {
		return handoffSocketStats{}, err
	}
	defer file.Close()
	address := fmt.Sprintf("%08X:%04X", binary.NativeEndian.Uint32(ip), local.Port)
	return parseHandoffSocketStats(file, address, inode)
}

func TestHandoffSocketStatsRequireExactIdentity(t *testing.T) {
	row := " 1: 0100007F:1234 00000000:0000 07 00000000:00000100 00:00000000 00000000 1000 0 123456 2 0000000000000000 7\n"
	stats, err := parseHandoffSocketStats(strings.NewReader(row), "0100007F:1234", "123456")
	if err != nil || stats.Drops != 7 || stats.QueueBytes != 256 || stats.Inode != "123456" {
		t.Fatalf("socket stats: %+v, %v", stats, err)
	}
	for _, invalid := range []string{"", row + row, strings.Replace(row, "123456", "123457", 1), strings.Replace(row, "00000100", "invalid", 1), strings.Replace(row, " 7\n", " unknown\n", 1)} {
		if _, err := parseHandoffSocketStats(strings.NewReader(invalid), "0100007F:1234", "123456"); err == nil {
			t.Fatalf("accepted missing/ambiguous/invalid identity: %q", invalid)
		}
	}
}

func TestHandoffSocketStatsDetectKernelReceiveOverflow(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	if err := public.SetReadBuffer(4096); err != nil {
		t.Fatal(err)
	}
	local := *public.LocalAddr().(*net.UDPAddr)
	before, err := readHandoffSocketStats(local, "")
	if err != nil || before.Drops != 0 {
		t.Fatalf("initial socket stats: %+v, %v", before, err)
	}
	client, err := net.DialUDP("udp4", nil, &local)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for range 256 {
		if _, err := client.Write(make([]byte, 1200)); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		after, err := readHandoffSocketStats(local, before.Inode)
		if err != nil {
			t.Fatal(err)
		}
		if after.Drops > 0 && after.QueueBytes > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("kernel receive overflow was not accounted")
}
