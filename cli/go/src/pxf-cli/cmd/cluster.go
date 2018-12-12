package cmd

import (
	"errors"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"os"
	"pxf-cli/pxf"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
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

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Sync PXF configs from master to all segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			doSetup()
			clusterRun(pxf.Sync)
		},
	}

	segHostList map[string]int
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
	clusterCmd.AddCommand(syncCmd)
}

func GetHostList(command pxf.Command) int {
	hostList := cluster.ON_HOSTS
	if command == pxf.Init {
		hostList = cluster.ON_HOSTS_AND_MASTER
	}
	return hostList
}

func GenerateHostList(cluster *cluster.Cluster) (map[string]int, error) {
	hostSegMap := make(map[string]int, 0)
	for contentID, seg := range cluster.Segments {
		if contentID == -1 {
			master, _ := operating.System.Hostname()
			if seg.Hostname != master {
				return nil, errors.New("ERROR: pxf cluster commands should only be run from Greenplum master")
			}
			continue
		}
		hostSegMap[seg.Hostname]++
	}
	return hostSegMap, nil
}

func GenerateStatusReport(command pxf.Command) string {
	cmdMsg := fmt.Sprintf(pxf.StatusMessage[command], len(segHostList))
	gplog.Info(cmdMsg)
	return cmdMsg
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
	err := connectionPool.Connect(1)
	if err != nil {
		gplog.Error(fmt.Sprintf("ERROR: Could not connect to GPDB.\n%s\n"+
			"Please make sure that your Greenplum database is running and you are on the master node.", err.Error()))
		os.Exit(1)
	}
	segConfigs := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfigs)
	segHostList, err = GenerateHostList(globalCluster)
	if err != nil {
		gplog.Error(err.Error())
		os.Exit(1)
	}
}

func clusterRun(command pxf.Command) error {
	defer connectionPool.Close()
	remoteCommand, err := pxf.RemoteCommandToRunOnSegments(command)
	if err != nil {
		gplog.Error(fmt.Sprintf("Error: %s", err))
		return err
	}

	cmdMsg := GenerateStatusReport(command)
	remoteOut := globalCluster.GenerateAndExecuteCommand(
		cmdMsg,
		func(contentID int) string {
			return remoteCommand
		},
		GetHostList(command))
	return GenerateOutput(command, remoteOut)
}
