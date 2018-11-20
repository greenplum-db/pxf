package pxf_test

import (
	"errors"
	"os"
	"pxf-cli/pxf"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MakeValidCliInputs", func() {
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

	It("Is successful when GPHOME is set and non-Init command is called", func() {
		inputs, err := pxf.MakeValidCliInputs(pxf.Start)
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome:  "/test/gphome",
			Cmd:     pxf.Start,
		}))
	})

	It("Init fails when PXF_CONF is not set", func() {
		os.Unsetenv("PXF_CONF")
		inputs, err := pxf.MakeValidCliInputs(pxf.Init)
		Expect(err).To(Equal(errors.New("PXF_CONF must be set.")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is not set", func() {
		os.Unsetenv("GPHOME")
		inputs, err := pxf.MakeValidCliInputs(pxf.Start)
		Expect(err).To(Equal(errors.New("GPHOME must be set.")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is blank", func() {
		os.Setenv("GPHOME", "")
		inputs, err := pxf.MakeValidCliInputs(pxf.Stop)
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank.")))
		Expect(inputs).To(BeNil())
	})
})

var _ = Describe("RemoteCommandToRunOnSegments", func() {
	It("constructs a list of shell args from the input", func() {
		inputs := &pxf.CliInputs{
			Gphome: "/test/gphome",
			Cmd:     pxf.Init,
		}
		expected := "/test/gphome/pxf/bin/pxf init"

		Expect(pxf.RemoteCommandToRunOnSegments(inputs)).To(Equal(expected))
	})
})
