package cmd_test

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"os/user"
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
	var (
		clusterOutput *cluster.RemoteOutput
		testStdout    *gbytes.Buffer
		testStderr    *gbytes.Buffer
	)

	BeforeEach(func() {
		_, _, testStdout, testStderr, _ = testhelper.SetupTestEnvironment()
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		configMaster := cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1"}
		configSegOne := cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0"}
		configSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1"}
		testCluster := cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo})
		cmd.SetCluster(testCluster)
		clusterOutput = &cluster.RemoteOutput{
			NumErrors: 0,
			Stderrs:   map[int]string{0: "", 1: "", 2: ""},
			Stdouts:   map[int]string{0: "everything fine", 1: "everything fine", 2: "everything fine"},
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
		Context("When we see the Catalina error on stop", func() {
			It("Reports all nodes stopped successfully", func() {
				catalinaStdout := "The stop command failed. Attempting to signal the process to stop through OS signal.\nTomcat stopped.\n"
				catalinaStderr := "Nov 24, 2018 5:25:59 PM org.apache.catalina.startup.Catalina stopServer\nSEVERE: No shutdown port configured. Shut down server through OS signal. Server not shut down.\n"
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 0,
					Stderrs:   map[int]string{0: catalinaStderr, 1: catalinaStderr},
					Stdouts:   map[int]string{0: catalinaStdout, 1: catalinaStdout},
				}
				cmd.GenerateOutput(pxf.Stop, clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 2 out of 2 nodes"))
			})
		})
	})
})
