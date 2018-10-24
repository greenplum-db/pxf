package test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os/exec"
	"pxf-cluster/gpssh"
)

var _ = Describe("generating a gpssh command", func() {
	It("specifies the hosts where the command runs as -f arguments", func() {
		var hostnames = []string { "abc", "def", "ghi" }
		var remoteCommand = []string { "echo", "hello" }

		var cmd = gpssh.Command(hostnames, remoteCommand)

		Expect(cmd).To(Equal(exec.Command("gpssh", "-h", "abc", "-h", "def", "-h", "ghi", "echo", "hello")))
	})
})
