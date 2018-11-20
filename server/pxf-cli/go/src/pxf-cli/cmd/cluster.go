package cmd

import (
	"fmt"
	"log"
	"os"
	"pxf-cli/pxf"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

const catalinaError = "SEVERE: No shutdown port configured"

var successMessage = map[pxf.Command]string{
	pxf.Init:  "PXF initialized successfully on %d out of %d nodes\n",
	pxf.Start: "PXF started successfully on %d out of %d nodes\n",
	pxf.Stop:  "PXF stopped successfully on %d out of %d nodes\n",
}

var errorMessage = map[pxf.Command]string{
	pxf.Init:  "PXF failed to initialize on %d out of %d nodes\n",
	pxf.Start: "PXF failed to start on %d out of %d nodes\n",
	pxf.Stop:  "PXF failed to stop on %d out of %d nodes\n",
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "perform <command> on each segment host in the cluster",
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize the local PXF server instance",
	Run: func(cmd *cobra.Command, args []string) {
		RunCluster(pxf.Init)
	},
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start the local PXF server instance",
	Run: func(cmd *cobra.Command, args []string) {
		RunCluster(pxf.Start)
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "stop the local PXF server instance",
	Run: func(cmd *cobra.Command, args []string) {
		RunCluster(pxf.Stop)
	},
}

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
}

func RunCluster(command pxf.Command) {
	// InitializeLogging must be called before we attempt to log with gplog.
	gplog.InitializeLogging("pxf_cli", "")

	inputs, err := pxf.MakeValidCliInputs(command)
	fatalOnError(err)

	connection := dbconn.NewDBConnFromEnvironment("postgres")
	fatalOnError(connection.Connect(1))
	defer connection.Close()

	segConfigs := cluster.MustGetSegmentConfiguration(connection)

	globalCluster := cluster.NewCluster(segConfigs)

	hostList := cluster.ON_HOSTS
	if command == pxf.Init {
		hostList = cluster.ON_HOSTS_AND_MASTER
	}
	remoteOut := globalCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Executing command '%s' on all hosts", command),
		func(contentID int) string {
			return pxf.RemoteCommandToRunOnSegments(inputs)
		},
		hostList)
	response := ""
	errCount := 0
	numHosts := len(remoteOut.Stderrs)
	for index, error := range remoteOut.Stderrs {
		host := globalCluster.Segments[index].Hostname
		if len(error) > 0 {
			if command == pxf.Stop && strings.Contains(error, catalinaError) {
				continue
			}
			response += fmt.Sprintf("%s ==> %s\n", host, error)
			errCount++
			continue
		}
	}
	if errCount == 0 {
		fmt.Printf(successMessage[command], numHosts-errCount, numHosts)
		os.Exit(0)
	}
	fmt.Printf("ERROR: "+errorMessage[command], errCount, numHosts)
	fmt.Printf("%s", response)
	os.Exit(1)
}

func fatalOnError(err error) {
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}
