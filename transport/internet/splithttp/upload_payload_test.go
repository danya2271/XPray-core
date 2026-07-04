package splithttp

import (
	"net/http"
	"testing"
)

func TestCollectIndexedHeaderValues(t *testing.T) {
	header := http.Header{}
	header.Set("X-Data-1", "b")
	header.Set("x-data-0", "a")
	header.Set("X-Data-3", "d")

	if got := collectIndexedHeaderValues(header, "X-Data-"); got != "ab" {
		t.Fatal("unexpected header payload:", got)
	}
}

func TestCollectIndexedCookieValues(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "http://example.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(&http.Cookie{Name: "x_data_1", Value: "b"})
	request.AddCookie(&http.Cookie{Name: "x_data_0", Value: "a"})
	request.AddCookie(&http.Cookie{Name: "x_data_3", Value: "d"})

	if got := collectIndexedCookieValues(request, "x_data_"); got != "ab" {
		t.Fatal("unexpected cookie payload:", got)
	}
}
