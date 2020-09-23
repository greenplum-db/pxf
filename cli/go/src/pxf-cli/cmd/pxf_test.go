package cmd_test

import (
	"bytes"
	"errors"
	"os"
	"pxf-cli/cmd"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CommandFunc", func() {
	Context("when GPHOME, JAVA_HOME, PXF_BASE and PXF_HOME are set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_BASE", "/test/somewhere/pxf_base")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
		})

		It("successfully generates init command", func() {
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			expected := "GPHOME=/test/gphome JAVA_HOME=/etc/java/home /test/pxfhome/bin/pxf init"
			Expect(expected).To(Equal(commandFunc("foo")))
		})
	})

	Context("when GPHOME is not set, JAVA_HOME, PXF_BASE, PXF_HOME, and PXF_CONF are set", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_BASE", "/test/somewhere/pxf_base")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
			_ = os.Setenv("PXF_CONF", "/test/pxfconf")
		})

		It("fails to init, register and migrate", func() {
			commandFunc, err := cmd.RegisterCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
		})

		It("it successfully generates start, stop, status, restart, reset, prepare, and migrate commands", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf start").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf stop").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf status").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf restart").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("/test/pxfhome/bin/pxf reset --force").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf prepare").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_CONF=/test/pxfconf PXF_BASE=/test/somewhere/pxf_base /test/pxfhome/bin/pxf migrate").To(Equal(commandFunc("foo")))
		})
	})

	Context("when PXF_BASE is not set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Unsetenv("PXF_BASE")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
			_ = os.Setenv("PXF_CONF", "/test/pxfconf")
		})

		It("successfully generates init and reset commands", func() {
			commandFunc, err := cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("/test/pxfhome/bin/pxf reset --force").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.InitCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("GPHOME=/test/gphome JAVA_HOME=/etc/java/home /test/pxfhome/bin/pxf init").To(Equal(commandFunc("foo")))
		})
		It("fails to start, stop, restart, status, sync, prepare, or migrate", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))

			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE must be set")))
		})
	})

	Context("when user specifies --delete", func() {
		BeforeEach(func() {
			_ = os.Setenv("PXF_BASE", "/test/somewhere/pxf_base")
			_ = os.Unsetenv("PXF_HOME")
			cmd.DeleteOnSync = true
		})
		It("sets up rsync commands of $PXF_BASE/{conf,lib,servers} dirs with --delete flag", func() {
			commandFunc, err := cmd.SyncCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(
				"rsync -az --delete -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_base/conf' " +
					"'/test/somewhere/pxf_base/lib' " +
					"'/test/somewhere/pxf_base/servers' " +
					"'sdw1:/test/somewhere/pxf_base'",
			))
			Expect(commandFunc("sdw2")).To(Equal(
				"rsync -az --delete -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_base/conf' " +
					"'/test/somewhere/pxf_base/lib' " +
					"'/test/somewhere/pxf_base/servers' " +
					"'sdw2:/test/somewhere/pxf_base'",
			))
		})
		AfterEach(func() {
			cmd.DeleteOnSync = false
		})
	})

	Context("when only PXF_BASE is set", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Unsetenv("PXF_HOME")
			_ = os.Setenv("PXF_BASE", "/test/somewhere/pxf_base")
			_ = os.Unsetenv("JAVA_HOME")
			_ = os.Unsetenv("PXF_CONF")
		})

		It("sets up rsync commands of $PXF_BASE/{conf,lib,servers} dirs", func() {
			commandFunc, err := cmd.SyncCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_base/conf' " +
					"'/test/somewhere/pxf_base/lib' " +
					"'/test/somewhere/pxf_base/servers' " +
					"'sdw1:/test/somewhere/pxf_base'",
			))
			Expect(commandFunc("sdw2")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_base/conf' " +
					"'/test/somewhere/pxf_base/lib' " +
					"'/test/somewhere/pxf_base/servers' " +
					"'sdw2:/test/somewhere/pxf_base'",
			))
		})

		It("fails to init, start, stop, restart, or tell status", func() {
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = cmd.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME must be set")))
		})
	})

	Context("when PXF_BASE is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_CONF", "/test/pxfconf")
			_ = os.Setenv("PXF_BASE", "")
			_ = os.Unsetenv("JAVA_HOME")
		})
		It("fails to start, stop, restart, status, or sync", func() {
			_ = os.Setenv("PXF_BASE", "")
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_BASE cannot be blank")))
		})
	})

	Context("when PXF_HOME is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "")
			_ = os.Unsetenv("PXF_BASE")
			_ = os.Unsetenv("JAVA_HOME")
		})
		It("it fails to init, start, stop, restart, register, or status", func() {
			_ = os.Setenv("PXF_HOME", "")
			commandFunc, err := cmd.RegisterCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_HOME cannot be blank")))
		})
	})

	Context("when PXF_BASE is the same as PXF_HOME", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_BASE", "/test/pxfhome")
			_ = os.Setenv("PXF_CONF", "/test/pxfconf")
			_ = os.Unsetenv("JAVA_HOME")
		})

		It("it fails to run the prepare command", func() {
			commandFunc, err := cmd.PrepareCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("the PXF_BASE value must be different from your PXF installation directory")))
		})

		It("it successfully generates start, stop, status, restart, reset, and migrate commands", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfhome /test/pxfhome/bin/pxf start").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfhome /test/pxfhome/bin/pxf stop").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfhome /test/pxfhome/bin/pxf status").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfhome /test/pxfhome/bin/pxf restart").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("/test/pxfhome/bin/pxf reset --force").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.MigrateCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_CONF=/test/pxfconf PXF_BASE=/test/pxfhome /test/pxfhome/bin/pxf migrate").To(Equal(commandFunc("foo")))
		})
	})

	Context("when PXF_CONF is the same as PXF_BASE", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_BASE", "/test/pxfconf")
			_ = os.Setenv("PXF_CONF", "/test/pxfconf")
			_ = os.Unsetenv("JAVA_HOME")
		})

		It("it fails to run the migrate command", func() {
			commandFunc, err := cmd.MigrateCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("your target PXF_BASE directory must be different from your existing PXF_CONF directory")))
		})

		It("it successfully generates start, stop, status, restart, and reset commands", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfconf /test/pxfhome/bin/pxf start").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfconf /test/pxfhome/bin/pxf stop").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfconf /test/pxfhome/bin/pxf status").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfconf /test/pxfhome/bin/pxf restart").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("/test/pxfhome/bin/pxf reset --force").To(Equal(commandFunc("foo")))

			commandFunc, err = cmd.PrepareCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect("PXF_BASE=/test/pxfconf /test/pxfhome/bin/pxf prepare").To(Equal(commandFunc("foo")))
		})
	})

	Context("when the user tries to run a warn command and they answer y", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("Y"))
			err := cmd.ResetCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

	Context("when the user tries to run a non-warn command", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("this input shouldn't matter!"))
			err := cmd.StatusCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

})
