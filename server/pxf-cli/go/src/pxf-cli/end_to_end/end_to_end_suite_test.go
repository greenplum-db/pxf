package end_to_end_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"os/exec"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

func RunCommand(cmd *exec.Cmd) []byte {
	output, _ := cmd.CombinedOutput()
	return output
}

/* This function is a helper function to execute pxf-cli and return a session
 * to allow checking its output.
 */
func runCli(commandStr string, args ...string) string {
	command := exec.Command(commandStr, args...)
	output := RunCommand(command)
	return fmt.Sprintf("%s", output)
}

var _ = Describe("pxf-cli", func() {

	pxfCliPath := fmt.Sprintf("%s/bin/pxf-cli", os.Getenv("GOPATH"))

	It("returns unknown command when invalid arg is given", func() {
		output := runCli(pxfCliPath, "invalid")
		Expect(output).To(ContainSubstring(`unknown command "invalid" for "pxf-cli"`))
	})

	It("returns help when no args are given", func() {
		output := runCli(pxfCliPath)
		Expect(output).To(ContainSubstring("Usage: pxf <command> [-y]"))
	})

	It("returns help when --help flag is given", func() {
		output := runCli(pxfCliPath, "--help")
		Expect(output).To(ContainSubstring("Usage: pxf <command> [-y]"))
	})

	It("returns help when --version flag is given", func() {
		output := runCli(pxfCliPath, "--version")
		Expect(output).To(ContainSubstring(fmt.Sprintf("PXF version")))
	})

	It("returns help when invalid cluster arg is given", func() {
		output := runCli(pxfCliPath, "cluster", "init1")
		Expect(output).To(ContainSubstring(fmt.Sprintf("perform <command> on each segment host in the cluster")))
	})
})
