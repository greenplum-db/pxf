package cmd_test

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
	"pxf-cli/cmd"
	"pxf-cli/pxf"
)

var _ = Describe("GetHostlist", func() {
	Context("When the command is init", func() {
		It("Hostlist includes all segment hosts + master", func() {
			Expect(cmd.GetHostlist(pxf.Init)).To(Equal(cluster.ON_HOSTS_AND_MASTER))
		})
	})

	Context("When the command is not init", func() {
		It("Hostlist includes only segment hosts", func() {
			Expect(cmd.GetHostlist(pxf.Start)).To(Equal(cluster.ON_HOSTS))
			Expect(cmd.GetHostlist(pxf.Stop)).To(Equal(cluster.ON_HOSTS))
		})
	})
})

var _ = Describe("GenerateOutput", func() {
	var clusterOutput *cluster.RemoteOutput

	BeforeEach(func() {
		configMaster := cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1"}
		configSegOne := cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0"}
		configSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1"}
		cmd.SetCluster(cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo}))
		clusterOutput = &cluster.RemoteOutput{
			NumErrors: 0,
			Stderrs:   map[int]string{-1: "", 0: "", 1: ""},
			Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "everything fine"},
			Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
		}
	})

	Describe("Running supported commands", func() {
		Context("When all nodes are successful", func() {
			It("Reports all nodes initialized successfully", func() {
				cmd.GenerateOutput(pxf.Init, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 3 out of 3 nodes"))
			})

			It("Reports all nodes started successfully", func() {
				cmd.GenerateOutput(pxf.Start, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF started successfully on 3 out of 3 nodes"))
			})

			It("Reports all nodes stopped successfully", func() {
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 nodes"))
			})
		})

		Context("When some nodes fail", func() {
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
			It("Reports the number of nodes that failed to initialize", func() {
				cmd.GenerateOutput(pxf.Init, clusterOutput)
				Expect(testStdout).Should(gbytes.Say("PXF failed to initialize on 1 out of 3 nodes"))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of nodes that failed to start", func() {
				cmd.GenerateOutput(pxf.Start, clusterOutput)
				Expect(testStdout).Should(gbytes.Say("PXF failed to start on 1 out of 3 nodes"))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of nodes that failed to stop", func() {
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 nodes"))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
		Context("When we see messages in Stderr, but NumErrors is 0", func() {
			It("Reports all nodes were successful", func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 0,
					Stderrs:   map[int]string{-1: "typical stderr", 0: "typical stderr", 1: "typical stderr"},
					Stdouts:   map[int]string{-1: "typical stdout", 0: "typical stdout", 1: "typical stdout"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
				}
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 nodes"))
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
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 nodes"))
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
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 nodes"))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
	})
})
