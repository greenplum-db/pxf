package test

import (
	"errors"
	. "github.com/onsi/ginkgo"
	"pxf-cluster/pxf"

	. "github.com/onsi/gomega"
	"os"
	//"pxf-cluster/pxf"
)

//var _ = Describe("CommandForSegments", func() {
//	gphome := "/the/gphome/dir"
//	args := []string{"init"}
//
//	Context("given valid inputs", func() {
//		It("is created without errors", func() {
//			err, cmd := pxf.CommandForSegments(gphome, args)
//			Expect(err).To(BeNil())
//			Expect(cmd).To(Equal([]string{"/the/gphome/dir/pxf/bin/pxf", "init"}))
//		})
//	})
//
//	Context("when no args are given", func() {
//		// it should error
//	})
//
//	Context("when extra args are given", func() {
//		// it should error
//	})
//
//	Context("when GPHOME is blank", func() {
//		// it should error
//	})
//})

var _ = Describe("MakeValidCliInputs", func() {
	var oldGphome string
	var isGphomeSet bool

	BeforeEach(func() {
		oldGphome, isGphomeSet = os.LookupEnv("GPHOME")
		os.Setenv("GPHOME", "/test/gphome")
	})

	AfterEach(func() {
		if isGphomeSet {
			os.Setenv("GPHOME", oldGphome)
		} else {
			os.Unsetenv("GPHOME")
		}
	})

	It("Is successful when GPHOME is set and args are valid", func() {
		err, inputs := pxf.MakeValidCliInputs([]string{"pxf-cluster", "init"})
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome: "/test/gphome",
			Args:   []string{"init"},
		}))
	})

	It("Fails when GPHOME is not set", func() {
		os.Unsetenv("GPHOME")
		err, inputs := pxf.MakeValidCliInputs([]string{"init"})
		Expect(err).To(Equal(errors.New("GPHOME is not set")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is blank", func() {
		os.Setenv("GPHOME", "")
		err, inputs := pxf.MakeValidCliInputs([]string{"init"})
		Expect(err).To(Equal(errors.New("GPHOME is blank")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when no arguments are passed", func() {
		err, inputs := pxf.MakeValidCliInputs(nil)
		Expect(err).To(Equal(errors.New("Usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when extra arguments are passed", func() {
		err, inputs := pxf.MakeValidCliInputs([]string {"pxf-cluster", "init", "abc"})
		Expect(err).To(Equal(errors.New("Usage: pxf cluster {start|stop|restart|init|status}")))
		Expect(inputs).To(BeNil())
	})
})
