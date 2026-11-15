//go:build xray_fakedns || xray_server

package conf

func init() {
	RegisterConfigureFilePostProcessingStage("FakeDNS", &FakeDNSPostProcessingStage{})
}
