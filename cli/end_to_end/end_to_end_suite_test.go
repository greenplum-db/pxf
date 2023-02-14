package end_to_end_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

/* This function is a helper function to execute pxf-cli and return a session
 * to allow checking its output.
 */
func runCli(commandStr string, args ...string) string {
	command := exec.Command(commandStr, args...)
	output, _ := command.CombinedOutput()
	return string(output)
}

var _ = Describe("pxf-cli", func() {

	pxfCliPath := "../build/pxf-cli"

	It("returns unknown command when invalid arg is given", func() {
		output := runCli(pxfCliPath, "invalid")
		Expect(output).To(ContainSubstring(`unknown command "invalid" for "pxf"`))
	})

	It("returns help when no args are given", func() {
		output := runCli(pxfCliPath)
		Expect(output).To(ContainSubstring("Usage: pxf cluster <command>"))
	})

	It("returns help when --help flag is given", func() {
		output := runCli(pxfCliPath, "--help")
		Expect(output).To(ContainSubstring("Usage: pxf cluster <command"))
	})

	It("returns help when --version flag is given", func() {
		output := runCli(pxfCliPath, "--version")
		Expect(output).To(ContainSubstring("PXF version"))
	})

	It("returns help when invalid cluster arg is given", func() {
		output := runCli(pxfCliPath, "cluster", "init1")
		Expect(output).To(ContainSubstring("Perform <command> on each segment host in the cluster"))
	})
})
