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
	Context("When there is no standby master", func() {
		It("reports master host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.InitCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Initializing PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on master host and 2 segment hosts..."))

			cmd.ResetCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Resetting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on master host and 2 segment hosts"))

			cmd.SyncCommand.GenerateStatusReport(createClusterData(2, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Syncing PXF configuration files from master host to 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from master host to 2 segment hosts"))

			cmd.RegisterCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Installing PXF extension on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on master host and 2 segment hosts"))

			cmd.PrepareCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Preparing PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on master host and 2 segment hosts"))

			cmd.MigrateCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Migrating PXF configuration on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on master host and 2 segment hosts"))
		})

		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.StartCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Starting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Starting PXF on master host and 2 segment hosts"))

			cmd.StopCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Stopping PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on master host and 2 segment hosts"))

			cmd.RestartCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Restarting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on master host and 2 segment hosts"))

			cmd.StatusCommand.GenerateStatusReport(createClusterData(3, clusterWithoutStandby))
			Expect(testLogFile).To(gbytes.Say("Checking status of PXF servers on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on master host and 2 segment hosts"))
		})
	})

	Context("When there is a standby master on its own host", func() {
		It("reports master host, standby master host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.InitCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Initializing PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on master host, standby master host, and 2 segment hosts"))

			cmd.ResetCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Resetting PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on master host, standby master host, and 2 segment hosts"))

			cmd.SyncCommand.GenerateStatusReport(createClusterData(3, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Syncing PXF configuration files from master host to standby master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from master host to standby master host and 2 segment hosts"))

			cmd.RegisterCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Installing PXF extension on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on master host, standby master host, and 2 segment hosts"))

			cmd.PrepareCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Preparing PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on master host, standby master host, and 2 segment hosts"))

			cmd.MigrateCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Migrating PXF configuration on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on master host, standby master host, and 2 segment hosts"))
		})

		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.StartCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Starting PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Starting PXF on master host, standby master host, and 2 segment hosts"))

			cmd.StopCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Stopping PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on master host, standby master host, and 2 segment hosts"))

			cmd.RestartCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Restarting PXF on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on master host, standby master host, and 2 segment hosts"))

			cmd.StatusCommand.GenerateStatusReport(createClusterData(4, clusterWithStandby))
			Expect(testLogFile).To(gbytes.Say("Checking status of PXF servers on master host, standby master host, and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on master host, standby master host, and 2 segment hosts"))
		})
	})

	Context("When there is a standby master on a segment host", func() {
		It("reports master host and segment hosts are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.InitCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Initializing PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on master host and 2 segment hosts"))

			cmd.ResetCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Resetting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on master host and 2 segment hosts"))

			cmd.SyncCommand.GenerateStatusReport(createClusterData(2, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Syncing PXF configuration files from master host to 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from master host to 2 segment hosts"))

			cmd.RegisterCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Installing PXF extension on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on master host and 2 segment hosts"))

			cmd.PrepareCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Preparing PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on master host and 2 segment hosts"))

			cmd.MigrateCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Migrating PXF configuration on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on master host and 2 segment hosts"))
		})

		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.StartCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Starting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Starting PXF on master host and 2 segment hosts"))

			cmd.StopCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Stopping PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on master host and 2 segment hosts"))

			cmd.RestartCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Restarting PXF on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on master host and 2 segment hosts"))

			cmd.StatusCommand.GenerateStatusReport(createClusterData(3, clusterWithStandbyOnSegHost))
			Expect(testLogFile).To(gbytes.Say("Checking status of PXF servers on master host and 2 segment hosts"))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on master host and 2 segment hosts"))
		})
	})

	Context("When there is only one segment host", func() {
		It("reports master host and segment host are initializing, resetting, registering, syncing, preparing, and migrating", func() {
			cmd.InitCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Initializing PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Initializing PXF on master host and 1 segment host"))

			cmd.ResetCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Resetting PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Resetting PXF on master host and 1 segment host"))

			cmd.SyncCommand.GenerateStatusReport(createClusterData(1, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Syncing PXF configuration files from master host to 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Syncing PXF configuration files from master host to 1 segment host"))

			cmd.RegisterCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Installing PXF extension on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Installing PXF extension on master host and 1 segment host"))

			cmd.PrepareCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Preparing PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Preparing PXF on master host and 1 segment host"))

			cmd.MigrateCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Migrating PXF configuration on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Migrating PXF configuration on master host and 1 segment host"))
		})

		It("reports segment hosts are starting, stopping, restarting and statusing", func() {
			cmd.StartCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Starting PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Starting PXF on master host and 1 segment host"))

			cmd.StopCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Stopping PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Stopping PXF on master host and 1 segment host"))

			cmd.RestartCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Restarting PXF on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Restarting PXF on master host and 1 segment host"))

			cmd.StatusCommand.GenerateStatusReport(createClusterData(2, clusterWithOneSeg))
			Expect(testLogFile).To(gbytes.Say("Checking status of PXF servers on master host and 1 segment host"))
			Expect(testStdout).To(gbytes.Say("Checking status of PXF servers on master host and 1 segment host"))
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
			_ = cmd.InitCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF initialized successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts started successfully", func() {
			_ = cmd.StartCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF started successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF started successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts restarted successfully", func() {
			_ = cmd.RestartCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF restarted successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF restarted successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts stopped successfully", func() {
			_ = cmd.StopCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts synced successfully", func() {
			_ = cmd.SyncCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF configs synced successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF configs synced successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts running", func() {
			_ = cmd.StatusCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF is running on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF is running on 3 out of 3 hosts"))
		})

		It("reports all hosts reset successfully", func() {
			_ = cmd.ResetCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF has been reset on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF has been reset on 3 out of 3 hosts"))
		})

		It("reports all hosts registered successfully", func() {
			_ = cmd.RegisterCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF extension has been installed on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF extension has been installed on 3 out of 3 hosts"))
		})

		It("reports all hosts prepared successfully", func() {
			_ = cmd.PrepareCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF prepared successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF prepared successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts migrated PXF successfully", func() {
			_ = cmd.MigrateCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF configuration migrated successfully on 3 out of 3 hosts"))
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
			_ = cmd.InitCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to initialize on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to initialize on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to start", func() {
			_ = cmd.StartCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to start on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to start on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to restart", func() {
			_ = cmd.RestartCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to restart on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to restart on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to stop", func() {
			_ = cmd.StopCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to sync", func() {
			_ = cmd.SyncCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF configs failed to sync on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF configs failed to sync on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that aren't running", func() {
			_ = cmd.StatusCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF is not running on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF is not running on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to reset", func() {
			_ = cmd.ResetCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("Failed to reset PXF on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("Failed to reset PXF on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to register", func() {
			_ = cmd.RegisterCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("Failed to install PXF extension on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("Failed to install PXF extension on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to prepare", func() {
			_ = cmd.PrepareCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to prepare on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to prepare on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to migrate", func() {
			_ = cmd.MigrateCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
			Expect(testStderr).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 3 hosts"))
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
			_ = cmd.StopCommand.GenerateOutput(clusterData)
			Expect(testLogFile).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
			Expect(testStderr).To(gbytes.Say(""))
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
			_ = cmd.StopCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> stderr line one\nstderr line two..."))
			Expect(testStdout).Should(gbytes.Say(""))
			Expect(testStderr).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
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
			_ = cmd.StopCommand.GenerateOutput(clusterData)
			Expect(testLogFile).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testLogFile).Should(gbytes.Say("sdw2 ==> something wrong on sdw2\nstderr line2..."))
			Expect(testStdout).Should(gbytes.Say(""))
			Expect(testStderr).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
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
				_ = cmd.InitCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF initialized successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 1 out of 1 host"))
			})

			It("reports host started successfully", func() {
				_ = cmd.StartCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF started successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF started successfully on 1 out of 1 host"))
			})

			It("reports host restarted successfully", func() {
				_ = cmd.RestartCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF restarted successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF restarted successfully on 1 out of 1 host"))
			})

			It("reports host stopped successfully", func() {
				_ = cmd.StopCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF stopped successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 1 out of 1 host"))
			})

			It("reports host synced successfully", func() {
				_ = cmd.SyncCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF configs synced successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF configs synced successfully on 1 out of 1 host"))
			})

			It("reports host running", func() {
				_ = cmd.StatusCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF is running on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF is running on 1 out of 1 host"))
			})

			It("reports host reset successfully", func() {
				_ = cmd.ResetCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF has been reset on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF has been reset on 1 out of 1 host"))
			})

			It("reports host registered successfully", func() {
				_ = cmd.RegisterCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF extension has been installed on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF extension has been installed on 1 out of 1 host"))
			})

			It("reports host prepared successfully", func() {
				_ = cmd.PrepareCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF prepared successfully on 1 out of 1 host"))
				Expect(testStdout).To(gbytes.Say("PXF prepared successfully on 1 out of 1 host"))
			})

			It("reports host migrated successfully", func() {
				_ = cmd.MigrateCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).To(gbytes.Say("PXF configuration migrated successfully on 1 out of 1 host"))
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
				_ = cmd.InitCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to initialize on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to initialize on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to start", func() {
				_ = cmd.StartCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to start on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to start on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to restart", func() {
				_ = cmd.RestartCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to restart on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to restart on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to stop", func() {
				_ = cmd.StopCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to stop on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to stop on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to sync", func() {
				_ = cmd.SyncCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF configs failed to sync on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF configs failed to sync on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that aren't running", func() {
				_ = cmd.StatusCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF is not running on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF is not running on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to reset", func() {
				_ = cmd.ResetCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("Failed to reset PXF on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("Failed to reset PXF on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to register", func() {
				_ = cmd.RegisterCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("Failed to install PXF extension on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("Failed to install PXF extension on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to prepare", func() {
				_ = cmd.PrepareCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to prepare on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to prepare on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})

			It("reports the number of hosts that failed to migrate", func() {
				_ = cmd.MigrateCommand.GenerateOutput(clusterDataWithOneHost)
				Expect(testLogFile).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 1 host"))
				Expect(testLogFile).Should(gbytes.Say("mdw ==> an error happened on mdw"))
				Expect(testStderr).Should(gbytes.Say("PXF failed to migrate configuration on 1 out of 1 host"))
				Expect(testStderr).Should(gbytes.Say("mdw ==> an error happened on mdw"))
			})
		})
	})
})
