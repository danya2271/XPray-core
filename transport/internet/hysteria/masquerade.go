package hysteria

import (
	"context"
	gotls "crypto/tls"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

func newMasqueradeHandler(config *Config) (http.Handler, error) {
	switch strings.ToLower(config.MasqType) {
	case "", "404":
		return http.NotFoundHandler(), nil
	case "file":
		return http.FileServer(http.Dir(config.MasqFile)), nil
	case "proxy":
		target, err := url.Parse(config.MasqUrl)
		if err != nil {
			return nil, err
		}
		transport := http.DefaultTransport.(*http.Transport)
		switch target.Scheme {
		case "http", "https":
			if config.MasqUrlInsecure {
				transport = transport.Clone()
				if transport.TLSClientConfig == nil {
					transport.TLSClientConfig = &gotls.Config{}
				}
				transport.TLSClientConfig.InsecureSkipVerify = true
			}
		case "", "unix":
			path := target.Path
			target = &url.URL{Scheme: "http", Host: "localhost"}
			dialer := &net.Dialer{Timeout: 30 * time.Second}
			transport = transport.Clone()
			transport.Proxy = nil
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", path)
			}
			transport.MaxIdleConns = masqueradeProxyMaxIdleConnections
			transport.MaxIdleConnsPerHost = masqueradeProxyMaxIdleConnsPerHost
		default:
			return nil, errors.New("unknown scheme")
		}
		return &httputil.ReverseProxy{
			Rewrite: func(request *httputil.ProxyRequest) {
				request.SetURL(target)
				request.Out.Trailer = request.In.Trailer
				if !config.MasqUrlRewriteHost {
					request.Out.Host = request.In.Host
				}
				if config.MasqUrlXForwarded {
					request.SetXForwarded()
				}
			},
			Transport:  transport,
			BufferPool: newMasqueradeProxyBufferPool(),
			ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
				errors.LogErrorInner(context.Background(), err, "HTTP reverse proxy error")
				writer.WriteHeader(http.StatusBadGateway)
			},
		}, nil
	case "string":
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			for name, value := range config.MasqStringHeaders {
				writer.Header().Set(name, value)
			}
			status := int(config.MasqStringStatusCode)
			if status == 0 {
				status = http.StatusOK
			}
			writer.WriteHeader(status)
			_, _ = writer.Write([]byte(config.MasqString))
		}), nil
	default:
		return nil, errors.New("unknown masq type")
	}
}
