//go:build xhysteria_native && cgo && (linux || android)

package hysteria_test

import "testing"

func TestHysteriaNativeHandoff(t *testing.T) {
	for _, test := range []struct {
		name    string
		nativeA bool
		nativeB bool
		masked  bool
	}{
		{"native-to-native", true, true, false},
		{"native-to-native-masked", true, true, true},
		{"native-to-go-masked", true, false, true},
		{"go-to-native-masked", false, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testHysteriaHandoff(t, test.nativeA, test.nativeB, test.masked)
		})
	}
}
