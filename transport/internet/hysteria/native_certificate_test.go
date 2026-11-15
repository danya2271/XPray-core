//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	cryptotls "crypto/tls"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type testCertificateUpdater struct {
	done    chan struct{}
	updates chan string
}

func (u *testCertificateUpdater) Done() <-chan struct{} { return u.done }
func (u *testCertificateUpdater) UpdateTLS(_ context.Context, certificate, key string) error {
	if _, err := cryptotls.X509KeyPair([]byte(certificate), []byte(key)); err != nil {
		return err
	}
	u.updates <- certificate
	return nil
}

func TestNativeCertificateWatcherWaitsForMatchingKeyPair(t *testing.T) {
	pair := func() ([]byte, []byte) {
		c, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate}),
			pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: c.PrivateKey})
	}
	certificate, key := pair()
	certPath, keyPath := filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem")
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(certPath, certificate)
	write(keyPath, key)
	u := &testCertificateUpdater{done: make(chan struct{}), updates: make(chan string, 2)}
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		watchNativeCertificate(u, &tls.Certificate{Certificate: certificate, Key: key, CertificatePath: certPath, KeyPath: keyPath}, 10*time.Millisecond)
	}()
	defer func() { close(u.done); <-exited }()
	updatedCert, updatedKey := pair()
	write(certPath, updatedCert)
	select {
	case <-u.updates:
		t.Fatal("partial certificate rotation reached the native engine")
	case <-time.After(40 * time.Millisecond):
	}
	write(keyPath, updatedKey)
	select {
	case got := <-u.updates:
		if got != string(updatedCert) {
			t.Fatal("wrong certificate reloaded")
		}
	case <-time.After(time.Second):
		t.Fatal("matching certificate/key were not reloaded")
	}
	select {
	case <-u.updates:
		t.Fatal("unchanged certificate was reloaded twice")
	case <-time.After(30 * time.Millisecond):
	}
}
