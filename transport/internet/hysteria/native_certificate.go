//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"bytes"
	"context"
	cryptotls "crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type nativeCertificateUpdater interface {
	Done() <-chan struct{}
	UpdateTLS(context.Context, string, string) error
}

func watchNativeCertificate(engine nativeCertificateUpdater, source *tls.Certificate, interval time.Duration) {
	certificate, key := source.Certificate, source.Key
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-engine.Done():
			return
		case <-ticker.C:
		}
		updatedCert, updatedKey := source.Certificate, source.Key
		var err error
		if source.CertificatePath != "" {
			updatedCert, err = os.ReadFile(source.CertificatePath)
		}
		if err == nil && source.KeyPath != "" {
			updatedKey, err = os.ReadFile(source.KeyPath)
		}
		if err == nil && bytes.Equal(certificate, updatedCert) && bytes.Equal(key, updatedKey) {
			continue
		}
		if err == nil {
			var pair cryptotls.Certificate
			pair, err = cryptotls.X509KeyPair(updatedCert, updatedKey)
			if err == nil {
				var normalized []byte
				normalized, err = x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
				if err == nil {
					err = engine.UpdateTLS(context.Background(), string(updatedCert), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: normalized})))
				}
			}
		}
		if err != nil {
			errors.LogWarning(context.Background(), "native Hysteria certificate reload failed: ", err)
			continue
		}
		certificate, key = updatedCert, updatedKey
	}
}
