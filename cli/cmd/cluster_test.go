package cmd_test

import (
	"pxf-cli/cmd"

	"github.com/greenplum-db/gp-common-go-libs/cluster"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

func createClusterData(numHosts int, cluster *cluster.Cluster) *cmd.ClusterData {
	return &cmd.ClusterData{
		Cluster:  cluster,
		NumHosts: numHosts,
		Output:   nil,
	}
}

var (
	configMaster                 = cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1", Role: "p"}
	configStandbyMaster          = cluster.SegConfig{ContentID: -1, Hostname: "smdw", DataDir: "/data/gpseg-1", Role: "m"}
	configStandbyMasterOnSegHost = cluster.SegConfig{ContentID: -1, Hostname: "sdw1", DataDir: "/data/gpseg-1", Role: "m"}
	configSegOne                 = cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0", Role: "p"}
	configSegTwo                 = cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1", Role: "p"}
	clusterWithoutStandby        = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo})
	clusterWithStandby           = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo, configStandbyMaster})
	clusterWithStandbyOnSegHost  = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo, configStandbyMasterOnSegHost})
	clusterWithOneSeg            = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configStandbyMasterOnSegHost})
	clusterWithOneHost           = cluster.NewCluster([]cluster.SegConfig{configMaster})
	clusterData                  = createClusterData(3, clusterWithoutStandby)
	clusterDataWithOneHost       = createClusterData(1, clusterWithOneHost)
)

var _ = Describe("GenerateStatusReport()", func() {
	Context("When there is no standby coordinator", func() {
		It("reports coordinator host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.GenerateStatusReport(&cmd.InitCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.ResetCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.SyncCommand, createClusterData(2, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from coordinator host to 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RegisterCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.PrepareCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.MigrateCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on coordinator host and 2 segment hosts"))
		})
		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.GenerateStatusReport(&cmd.StartCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Starting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StopCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RestartCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StatusCommand, createClusterData(3, clusterWithoutStandby))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on coordinator host and 2 segment hosts"))
		})
	})
	Context("When there is a standby coordinator on its own host", func() {
		It("reports coordinator host, standby coordinator host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.GenerateStatusReport(&cmd.InitCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.ResetCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.SyncCommand, createClusterData(3, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from coordinator host to standby coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RegisterCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.PrepareCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.MigrateCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on coordinator host, standby coordinator host, and 2 segment hosts"))
		})
		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.GenerateStatusReport(&cmd.StartCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Starting PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StopCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RestartCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on coordinator host, standby coordinator host, and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StatusCommand, createClusterData(4, clusterWithStandby))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on coordinator host, standby coordinator host, and 2 segment hosts"))
		})
	})
	Context("When there is a standby coordinator on a segment host", func() {
		It("reports coordinator host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.GenerateStatusReport(&cmd.InitCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.ResetCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.SyncCommand, createClusterData(2, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from coordinator host to 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RegisterCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.PrepareCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.MigrateCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on coordinator host and 2 segment hosts"))
		})
		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.GenerateStatusReport(&cmd.StartCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Starting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StopCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.RestartCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on coordinator host and 2 segment hosts"))
			cmd.GenerateStatusReport(&cmd.StatusCommand, createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on coordinator host and 2 segment hosts"))
		})
	})
	Context("When there is only one segment host", func() {
		It("reports coordinator host and segment host are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.GenerateStatusReport(&cmd.InitCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.ResetCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.SyncCommand, createClusterData(1, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from coordinator host to 1 segment host"))
			cmd.GenerateStatusReport(&cmd.RegisterCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.PrepareCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.MigrateCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on coordinator host and 1 segment host"))
		})
		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.GenerateStatusReport(&cmd.StartCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Starting PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.StopCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.RestartCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on coordinator host and 1 segment host"))
			cmd.GenerateStatusReport(&cmd.StatusCommand, createClusterData(2, clusterWithOneSeg))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on coordinator host and 1 segment host"))
		})
	})
})

var _ = Describe("GenerateOutput()", func() {
	BeforeEach(func() {
		clusterData.Output = &cluster.RemoteOutput{
			NumErrors: 0,
			FailedCommands: []*cluster.ShellCommand{
				nil,
				nil,
				nil,
			},
			Commands: []cluster.ShellCommand{
				{
					Host:   "mdw",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
				{
					Host:   "sdw1",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
				{
					Host:   "sdw2",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
			},
		}
	})
	Context("when all hosts are successful", func() {
		It("reports all hosts initialized successfully", func() {
			_ = cmd.GenerateOutput(&cmd.InitCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts started successfully", func() {
			_ = cmd.GenerateOutput(&cmd.StartCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF started successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts restarted successfully", func() {
			_ = cmd.GenerateOutput(&cmd.RestartCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF restarted successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts stopped successfully", func() {
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts synced successfully", func() {
			_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF configs synced successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts running", func() {
			_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF is running on 3 out of 3 hosts"))
		})

		It("reports all hosts reset successfully", func() {
			_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF has been reset on 3 out of 3 hosts"))
		})

		It("reports all hosts registered successfully", func() {
			_ = cmd.GenerateOutput(&cmd.RegisterCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF extension has been installed on 3 out of 3 hosts"))
		})

		It("reports all hosts prepared successfully", func() {
			_ = cmd.GenerateOutput(&cmd.PrepareCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF prepared successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts migrated PXF successfully", func() {
			_ = cmd.GenerateOutput(&cmd.MigrateCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF configuration migrated successfully on 3 out of 3 hosts"))
		})
	})

	Context("when some hosts fail", func() {
		BeforeEach(func() {
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "",
				Stderr: "an error happened on sdw2",
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
		})
		It("reports the number of hosts that failed to initialize", func() {
			_ = cmd.GenerateOutput(&cmd.InitCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to initialize on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to start", func() {
			_ = cmd.GenerateOutput(&cmd.StartCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to start on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to restart", func() {
			_ = cmd.GenerateOutput(&cmd.RestartCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to restart on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to stop", func() {
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to sync", func() {
			_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF configs failed to sync on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that aren't running", func() {
			_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF is not running on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to reset", func() {
			_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("Failed to reset PXF on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to register", func() {
			_ = cmd.GenerateOutput(&cmd.RegisterCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("Failed to install PXF extension on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to prepare", func() {
			_ = cmd.GenerateOutput(&cmd.PrepareCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to prepare on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to migrate", func() {
			_ = cmd.GenerateOutput(&cmd.MigrateCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})
	})

	Context("when we see messages in Stderr, but NumErrors is 0", func() {
		It("reports all hosts were successful", func() {
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 0,
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
					{
						Host:   "sdw2",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
		})
	})

	Context("when a command fails, and output is multiline", func() {
		It("truncates the output to two lines", func() {
			stderr := `stderr line one
stderr line two
stderr line three`
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "everything not fine",
				Stderr: stderr,
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> stderr line one\nstderr line two..."))
		})
	})

	Context("when NumErrors is non-zero, but Stderr is empty", func() {
		It("reports Stdout in error message", func() {
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "something wrong on sdw2\nstderr line2\nstderr line3",
				Stderr: "",
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> something wrong on sdw2\nstderr line2..."))
		})
	})

	Context("when only one host gets acted on", func() {
		BeforeEach(func() {
			clusterDataWithOneHost.Output = &cluster.RemoteOutput{
				NumErrors: 0,
				FailedCommands: []*cluster.ShellCommand{
					nil,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
				},
			}
		})
		Context("when host is successful", func() {
			It("reports host initialized successfully", func() {
				_ = cmd.GenerateOutput(&cmd.InitCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 1 out of 1 host"))
			})

			It("reports host started successfully", func() {
				_ = cmd.GenerateOutput(&cmd.StartCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF started successfully on 1 out of 1 host"))
			})

			It("reports host restarted successfully", func() {
				_ = cmd.GenerateOutput(&cmd.RestartCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF restarted successfully on 1 out of 1 host"))
			})

			It("reports host stopped successfully", func() {
				_ = cmd.GenerateOutput(&cmd.StopCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 1 out of 1 host"))
			})

			It("reports host synced successfully", func() {
				_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF configs synced successfully on 1 out of 1 host"))
			})

			It("reports host running", func() {
				_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF is running on 1 out of 1 host"))
			})

			It("reports host reset successfully", func() {
				_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF has been reset on 1 out of 1 host"))
			})

			It("reports host registered successfully", func() {
				_ = cmd.GenerateOutput(&cmd.RegisterCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF extension has been installed on 1 out of 1 host"))
			})

			It("reports host prepared successfully", func() {
				_ = cmd.GenerateOutput(&cmd.PrepareCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF prepared successfully on 1 out of 1 host"))
			})

			It("reports host migrated successfully", func() {
				_ = cmd.GenerateOutput(&cmd.MigrateCommand, clusterDataWithOneHost)
				Expect(testStdout).To(gbytes.Say("PXF configuration migrated successfully on 1 out of 1 host"))
			})
		})
		Context("when host is not successful", func() {
			BeforeEach(func() {
				failedCommand := cluster.ShellCommand{
					Host:   "mdw",
					Stdout: "",
					Stderr: "an error happened on mdw",
					Error:  errors.New("some error"),
				}
				clusterDataWithOneHost.Output = &cluster.RemoteOutput{
					NumErrors: 1,
					FailedCommands: []*cluster.ShellCommand{
						&failedCommand,
					},
					Commands: []cluster.ShellCommand{
						failedCommand,
					},
				}
			})
			It("reports the number of hosts that failed to initialize", func() {
				_ = cmd.GenerateOutput(&cmd.InitCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to initialize on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to start", func() {
				_ = cmd.GenerateOutput(&cmd.StartCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to start on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to restart", func() {
				_ = cmd.GenerateOutput(&cmd.RestartCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to restart on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to stop", func() {
				_ = cmd.GenerateOutput(&cmd.StopCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to sync", func() {
				_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF configs failed to sync on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that aren't running", func() {
				_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF is not running on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to reset", func() {
				_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("Failed to reset PXF on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to register", func() {
				_ = cmd.GenerateOutput(&cmd.RegisterCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("Failed to install PXF extension on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to prepare", func() {
				_ = cmd.GenerateOutput(&cmd.PrepareCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to prepare on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to migrate", func() {
				_ = cmd.GenerateOutput(&cmd.MigrateCommand, clusterDataWithOneHost)
				Expect(testStdout).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})
		})
	})
})
