package pxf_test

import (
	"errors"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"pxf-cli/pxf"
)

var _ = Describe("RemoteCommandToRunOnSegments", func() {
	BeforeEach(func() {
		_ = os.Setenv("GPHOME", "/test/gphome")
		_ = os.Setenv("PXF_CONF", "/test/gphome/pxf_conf")
	})

	It("Is successful when GPHOME and PXF_CONF are set and init is called", func() {
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(err).To(BeNil())
		expected := "PXF_CONF=/test/gphome/pxf_conf /test/gphome/pxf/bin/pxf init"
		Expect(command).To(Equal(expected))
	})

	It("Is successful when GPHOME is set and start/stop are called", func() {
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Start)
		Expect(err).To(BeNil())
		Expect(command).To(Equal("/test/gphome/pxf/bin/pxf start"))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Stop)
		Expect(err).To(BeNil())
		Expect(command).To(Equal("/test/gphome/pxf/bin/pxf stop"))
	})

	It("Fails to init when PXF_CONF is not set", func() {
		_ = os.Unsetenv("PXF_CONF")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
	})

	It("Fails to init when PXF_CONF is blank", func() {
		_ = os.Setenv("PXF_CONF", "")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
	})

	It("Fails to init, start, sync, or stop when GPHOME is not set", func() {
		_ = os.Unsetenv("GPHOME")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Start)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Sync)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Stop)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
	})

	It("Fails to init, start, sync, or stop when GPHOME is blank", func() {
		_ = os.Setenv("GPHOME", "")
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Init)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Start)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Sync)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		command, err = pxf.RemoteCommandToRunOnSegments(pxf.Stop)
		Expect(command).To(Equal(""))
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
	})

	It("Appends the master hostname when syncing", func() {
		operating.System.Hostname = func() (string, error) {
			return "fake-host", nil
		}
		command, err := pxf.RemoteCommandToRunOnSegments(pxf.Sync)
		Expect(err).To(BeNil())
		Expect(command).To(Equal("/test/gphome/pxf/bin/pxf sync fake-host"))
	})
})
