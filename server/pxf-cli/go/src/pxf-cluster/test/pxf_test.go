package test

import (
	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"pxf-cluster/pxf"
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

	validArgs := []string{"pxf-cluster", "init"}
	It("Is successful when GPHOME is set and args are valid", func() {
		inputs, err := pxf.MakeValidCliInputs(validArgs)
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome: "/test/gphome",
			PxfConf: "/test/gphome/pxf_conf",
			Args:   []string{"init"},
		}))
	})

	It("Init fails when PXF_CONF is not set", func() {
		os.Unsetenv("PXF_CONF")
		inputs, err := pxf.MakeValidCliInputs(validArgs)
		Expect(err).To(Equal(errors.New("PXF_CONF must be set.")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is not set", func() {
		os.Unsetenv("GPHOME")
		inputs, err := pxf.MakeValidCliInputs(validArgs)
		Expect(err).To(Equal(errors.New("GPHOME must be set.")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is blank", func() {
		os.Setenv("GPHOME", "")
		inputs, err := pxf.MakeValidCliInputs(validArgs)
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank.")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when args is nil", func() {
		inputs, err := pxf.MakeValidCliInputs(nil)
		Expect(err).To(Equal(errors.New("usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when no arguments are passed", func() {
		inputs, err := pxf.MakeValidCliInputs([]string{"pxf-cluster"})
		Expect(err).To(Equal(errors.New("usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when extra arguments are passed", func() {
		inputs, err := pxf.MakeValidCliInputs([]string{"pxf-cluster", "init", "abc"})
		Expect(err).To(Equal(errors.New("usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when the subcommand is not valid", func() {
		inputs, err := pxf.MakeValidCliInputs([]string{"pxf-cluster", "invalid"})
		Expect(err).To(Equal(errors.New("usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})
})

var _ = Describe("RemoteCommandToRunOnSegments", func() {
	It("constructs a list of shell args from the input", func() {
		inputs := &pxf.CliInputs{
			Gphome: "/test/gphome",
			Args:   []string{"init"},
		}
		expected := []string{"/test/gphome/pxf/bin/pxf", "init"}

		Expect(pxf.RemoteCommandToRunOnSegments(inputs)).To(Equal(expected))
	})
})
