package hysteria

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMasqueradeProxyPreservesRequestTrailers(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			t.Error(err)
		}
		writer.Header().Set("X-Request-Trailer", request.Trailer.Get("X-Upload-Complete"))
	}))
	defer upstream.Close()
	handler, err := newMasqueradeHandler(&Config{MasqType: "proxy", MasqUrl: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "https://cover.example/upload", bytes.NewBufferString("body"))
	request.ContentLength = -1
	request.Trailer = http.Header{"X-Upload-Complete": {"yes"}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Request-Trailer") != "yes" {
		t.Fatalf("request trailers were lost: status=%d headers=%v", response.Code, response.Header())
	}
}
