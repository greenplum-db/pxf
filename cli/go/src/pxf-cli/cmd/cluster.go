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

var (
	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Perform <command> on each segment host in the cluster",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Init)
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Start)
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "Stop the local PXF server instance",
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
	numHosts := len(remoteOut.Stderrs)
	numErrors := remoteOut.NumErrors
	if numErrors == 0 {
		gplog.Info(pxf.SuccessMessage[command], numHosts-numErrors, numHosts)
		return nil
	}
	response := ""
	for index, stderr := range remoteOut.Stderrs {
		if remoteOut.Errors[index] == nil {
			continue
		}
		host := globalCluster.Segments[index].Hostname
		errorMessage := stderr
		if len(errorMessage) == 0 {
			errorMessage = remoteOut.Stdouts[index]
		}
		lines := strings.Split(errorMessage, "\n")
		errorMessage = lines[0]
		if len(lines) > 1 {
			errorMessage += "\n" + lines[1]
		}
		if len(lines) > 2 {
			errorMessage += "..."
		}
		response += fmt.Sprintf("%s ==> %s\n", host, errorMessage)
	}
	gplog.Info("ERROR: "+pxf.ErrorMessage[command], numErrors, numHosts)
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
