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
	Context("when GPHOME, JAVA_HOME, PXF_CONF and PXF_HOME are set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_CONF", "/test/somewhere/pxf_conf")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
		})

		It("successfully generates init command", func() {
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			expected := "GPHOME=/test/gphome PXF_CONF=/test/somewhere/pxf_conf JAVA_HOME=/etc/java/home /test/pxfhome/bin/pxf init"
			Expect(commandFunc("foo")).To(Equal(expected))
		})
	})

	Context("when GPHOME is not set, JAVA_HOME, PXF_CONF and PXF_HOME are set", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_CONF", "/test/somewhere/pxf_conf")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
		})

		It("fails to init, fails to register", func() {
			commandFunc, err := cmd.RegisterCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
		})

		It("successfully generates start, stop, status, restart, and reset commands", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf start"))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf stop"))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf status"))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf restart"))

			commandFunc, err = cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf reset --force"))
		})
	})

	Context("when PXF_CONF is not set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Unsetenv("PXF_CONF")
			_ = os.Unsetenv("JAVA_HOME")
		})

		It("successfully generates start, stop, status, restart, and reset commands", func() {
			commandFunc, err := cmd.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf start"))

			commandFunc, err = cmd.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf stop"))

			commandFunc, err = cmd.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf status"))

			commandFunc, err = cmd.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf restart"))

			commandFunc, err = cmd.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/pxfhome/bin/pxf reset --force"))
		})
		It("fails to init or sync", func() {
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF must be set")))

			commandFunc, err = cmd.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
		})
	})
	Context("when user specifies --delete", func() {
		BeforeEach(func() {
			_ = os.Setenv("PXF_CONF", "/test/somewhere/pxf_conf")
			_ = os.Unsetenv("PXF_HOME")
			cmd.DeleteOnSync = true
		})
		It("sets up rsync commands of $PXF_CONF/{conf,lib,servers} dirs with --delete flag", func() {
			commandFunc, err := cmd.SyncCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(
				"rsync -az --delete -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_conf/conf' " +
					"'/test/somewhere/pxf_conf/lib' " +
					"'/test/somewhere/pxf_conf/servers' " +
					"'sdw1:/test/somewhere/pxf_conf'",
			))
			Expect(commandFunc("sdw2")).To(Equal(
				"rsync -az --delete -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_conf/conf' " +
					"'/test/somewhere/pxf_conf/lib' " +
					"'/test/somewhere/pxf_conf/servers' " +
					"'sdw2:/test/somewhere/pxf_conf'",
			))
		})
		AfterEach(func() {
			cmd.DeleteOnSync = false
		})
	})
	Context("when user specifies --skip-register flag during initialization", func() {
		BeforeEach(func() {
			_ = os.Setenv("PXF_CONF", "/test/somewhere/pxf_conf")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("JAVA_HOME", "/etc/java/home")
			cmd.SkipRegisterOnInit = true
		})
		It("initializes PXF with the --skip-register flag", func() {
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			expected := "GPHOME=/test/gphome PXF_CONF=/test/somewhere/pxf_conf JAVA_HOME=/etc/java/home /test/pxfhome/bin/pxf init --skip-register"
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(expected))
			Expect(commandFunc("sdw2")).To(Equal(expected))
		})
		AfterEach(func() {
			cmd.SkipRegisterOnInit = false
		})
	})
	Context("when only PXF_CONF is set", func() {
		BeforeEach(func() {
			_ = os.Unsetenv("GPHOME")
			_ = os.Unsetenv("PXF_HOME")
			_ = os.Setenv("PXF_CONF", "/test/somewhere/pxf_conf")
			_ = os.Unsetenv("JAVA_HOME")
		})

		It("sets up rsync commands of $PXF_CONF/{conf,lib,servers} dirs", func() {
			commandFunc, err := cmd.SyncCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_conf/conf' " +
					"'/test/somewhere/pxf_conf/lib' " +
					"'/test/somewhere/pxf_conf/servers' " +
					"'sdw1:/test/somewhere/pxf_conf'",
			))
			Expect(commandFunc("sdw2")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/somewhere/pxf_conf/conf' " +
					"'/test/somewhere/pxf_conf/lib' " +
					"'/test/somewhere/pxf_conf/servers' " +
					"'sdw2:/test/somewhere/pxf_conf'",
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
		})
	})

	Context("when PXF_CONF is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "/test/pxfhome")
			_ = os.Setenv("PXF_CONF", "")
			_ = os.Unsetenv("JAVA_HOME")
		})
		It("fails to init or sync", func() {
			_ = os.Setenv("PXF_CONF", "")
			commandFunc, err := cmd.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
			commandFunc, err = cmd.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
		})
	})

	Context("when PXF_HOME is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_HOME", "")
			_ = os.Unsetenv("PXF_CONF")
			_ = os.Unsetenv("JAVA_HOME")
		})
		It("fails to init, start, stop, restart, register, or status", func() {
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
		})
	})

	Context("When the user tries to run a warn command and doesn't answer y", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte(""))
			err := cmd.ResetCommand.Warn(&input)
			Expect(err).To(Equal(errors.New("pxf reset cancelled")))
		})
	})

	Context("When the user tries to run a warn command and they answer y", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("Y"))
			err := cmd.ResetCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

	Context("When the user tries to run a non-warn command", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("this input shouldn't matter!"))
			err := cmd.StatusCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

})
