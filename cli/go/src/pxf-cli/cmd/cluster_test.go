package cmd_test

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"pxf-cli/cmd"
	"pxf-cli/pxf"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

var _ = Describe("GetHostlist", func() {
	Context("When the command is init", func() {
		It("Hostlist includes all segment hosts + master", func() {
			Expect(cmd.GetHostList(pxf.Init)).To(Equal(cluster.ON_HOSTS_AND_MASTER))
		})
	})

	Context("When the command is not init", func() {
		It("Hostlist includes only segment hosts", func() {
			Expect(cmd.GetHostList(pxf.Start)).To(Equal(cluster.ON_HOSTS))
			Expect(cmd.GetHostList(pxf.Stop)).To(Equal(cluster.ON_HOSTS))
		})
	})
})

var (
	clusterOutput *cluster.RemoteOutput
	configMaster  = cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1"}
	configSegOne  = cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0"}
	configSegTwo  = cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1"}
	globalCluster = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo})
)

var _ = Describe("GenerateHostList", func() {
	It("Returns the correct hostlist and master hostname", func() {
		operating.System.Hostname = func() (string, error) {
			return "mdw", nil
		}
		hostlist, err := cmd.GenerateHostList(globalCluster)
		Expect(err).To(BeNil())
		Expect(hostlist).To(Equal(map[string]int{"sdw1": 1, "sdw2": 1}))
	})
	It("Errors if not running from master host", func() {
		operating.System.Hostname = func() (string, error) {
			return "fake-host", nil
		}
		hostlist, err := cmd.GenerateHostList(globalCluster)
		Expect(err.Error()).To(Equal("ERROR: pxf cluster commands should only be run from Greenplum master"))
		Expect(hostlist).To(BeNil())
	})
})

var _ = Describe("GenerateOutput", func() {
	cmd.SetCluster(globalCluster)
	clusterOutput = &cluster.RemoteOutput{
		NumErrors: 0,
		Stderrs:   map[int]string{-1: "", 0: "", 1: ""},
		Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "everything fine"},
		Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
	}
	Describe("Running supported commands", func() {
		Context("When all hosts are successful", func() {
			It("Reports all hosts initialized successfully", func() {
				_ = cmd.GenerateOutput(pxf.Init, clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.SuccessMessage[pxf.Init], 3, 3)))
			})

			It("Reports all hosts started successfully", func() {
				_ = cmd.GenerateOutput(pxf.Start, clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.SuccessMessage[pxf.Start], 3, 3)))
			})

			It("Reports all hosts stopped successfully", func() {
				_ = cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.SuccessMessage[pxf.Stop], 3, 3)))
			})

			It("Reports all hosts synced successfully", func() {
				_ = cmd.GenerateOutput(pxf.Sync, clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.SuccessMessage[pxf.Sync], 3, 3)))
			})
		})

		Context("When some hosts fail", func() {
			var expectedError string
			BeforeEach(func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: "an error happened on sdw2"},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "something wrong"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError = "sdw2 ==> an error happened on sdw2"
			})
			It("Reports the number of hosts that failed to initialize", func() {
				_ = cmd.GenerateOutput(pxf.Init, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Init], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to start", func() {
				_ = cmd.GenerateOutput(pxf.Start, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Start], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to stop", func() {
				_ = cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Stop], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to sync", func() {
				_ = cmd.GenerateOutput(pxf.Sync, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Sync], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
		Context("Before the command returns", func() {
			cmd.SetCluster(globalCluster)
			hostList := map[string]int{"sdw1": 1, "sdw2": 2}
			cmd.SetHostList(hostList)
			It("Reports the number of hosts that will be initialized", func() {
				_ = cmd.GenerateStatusReport(pxf.Init)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.StatusMessage[pxf.Init], 2)))
			})

			It("Reports the number of hosts will be started", func() {
				_ = cmd.GenerateStatusReport(pxf.Start)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.StatusMessage[pxf.Start], 2)))
			})

			It("Reports the number of hosts that will be stopped", func() {
				_ = cmd.GenerateStatusReport(pxf.Stop)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.StatusMessage[pxf.Stop], 2)))
			})

			It("Reports the number of hosts that will be synced", func() {
				_ = cmd.GenerateStatusReport(pxf.Sync)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.StatusMessage[pxf.Sync], 2)))
			})
		})
		Context("When we see messages in Stderr, but NumErrors is 0", func() {
			It("Reports all hosts were successful", func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 0,
					Stderrs:   map[int]string{-1: "typical stderr", 0: "typical stderr", 1: "typical stderr"},
					Stdouts:   map[int]string{-1: "typical stdout", 0: "typical stdout", 1: "typical stdout"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
				}
				_ = cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
			})
		})

		Context("When a command fails, and output is multiline", func() {
			It("Truncates the output to two lines", func() {
				stderr := `stderr line one
stderr line two
stderr line three`
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: stderr},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "everything not fine"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError := `sdw2 ==> stderr line one
stderr line two...`
				_ = cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Stop], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
		Context("When NumErrors is non-zero, but Stderr is empty", func() {
			It("Reports Stdout in error message", func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: ""},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "something wrong on sdw2\nstderr line2\nstderr line3"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError := "sdw2 ==> something wrong on sdw2\nstderr line2..."
				_ = cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.ErrorMessage[pxf.Stop], 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
	})
})
