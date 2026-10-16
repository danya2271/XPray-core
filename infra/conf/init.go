//go:build xray_fakedns

package conf

func init() {
	RegisterConfigureFilePostProcessingStage("FakeDNS", &FakeDNSPostProcessingStage{})
}
