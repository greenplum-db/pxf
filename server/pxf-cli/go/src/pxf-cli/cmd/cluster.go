package cmd

import (
	"errors"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
	"pxf-cli/pxf"
	"strings"
)

const catalinaError = "SEVERE: No shutdown port configured"

var (
	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "perform <command> on each segment host in the cluster",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "initialize the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Init)
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Start)
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Stop)
		},
	}
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
}

func GetHostlist(command pxf.Command) int {
	hostlist := cluster.ON_HOSTS
	if command == pxf.Init {
		hostlist = cluster.ON_HOSTS_AND_MASTER
	}
	return hostlist
}

func GenerateOutput(command pxf.Command, remoteOut *cluster.RemoteOutput) error {
	response := ""
	errCount := 0
	numHosts := len(remoteOut.Stderrs)
	for index, err := range remoteOut.Stderrs {
		host := globalCluster.Segments[index].Hostname
		if len(err) > 0 {
			if command == pxf.Stop && strings.Contains(err, catalinaError) {
				continue
			}
			response += fmt.Sprintf("%s ==> %s\n", host, err)
			errCount++
			continue
		}
	}
	if errCount == 0 {
		gplog.Info(pxf.SuccessMessage[command], numHosts-errCount, numHosts)
		return nil
	}
	gplog.Info("ERROR: "+pxf.ErrorMessage[command], errCount, numHosts)
	gplog.Error("%s", response)
	return errors.New(response)
}

func doSetup() {
	connectionPool = dbconn.NewDBConnFromEnvironment("postgres")
	connectionPool.MustConnect(1)
	segConfigs = cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfigs)
}

func clusterRun(command pxf.Command) error {
	defer connectionPool.Close()
	remoteCommand, err := pxf.RemoteCommandToRunOnSegments(command)
	if err != nil {
		gplog.Error(fmt.Sprintf("Error: %s", err))
		return err
	}

	remoteOut := globalCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Executing command '%s' on all hosts", string(command)),
		func(contentID int) string {
			return remoteCommand
		},
		GetHostlist(command))
	return GenerateOutput(command, remoteOut)
}
