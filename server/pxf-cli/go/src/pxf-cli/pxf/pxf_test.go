package pxf_test

import (
	"errors"
	"os"
	"pxf-cli/pxf"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RemoteCommandToRunOnSegments", func() {
	var oldGphome string
	var isGphomeSet bool

	BeforeEach(func() {
		oldGphome, isGphomeSet = os.LookupEnv("GPHOME")
		os.Setenv("GPHOME", "/test/gphome")
		os.Setenv("PXF_CONF", "/test/gphome/pxf_conf")
	})

	AfterEach(func() {
		if isGphomeSet {
			os.Setenv("GPHOME", oldGphome)
		} else {
			os.Unsetenv("GPHOME")
		}
	})

	It("constructs a list of shell args from the input", func() {
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(err).To(BeNil())
		expected := "PXF_CONF=/test/gphome/pxf_conf /test/gphome/pxf/bin/pxf init"
		Expect(command).To(Equal(expected))
	})

	It("Is successful when GPHOME is set and start command is called", func() {
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Start)
		Expect(err).To(BeNil())
		Expect(command).To(Equal("/test/gphome/pxf/bin/pxf start"))
	})

	It("Init fails when PXF_CONF is not set", func() {
		os.Unsetenv("PXF_CONF")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
	})

	It("Fails when GPHOME is not set", func() {
		os.Unsetenv("GPHOME")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
	})

	It("Fails when GPHOME is blank", func() {
		os.Setenv("GPHOME", "")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
	})
})
