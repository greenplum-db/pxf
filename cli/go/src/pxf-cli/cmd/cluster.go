package cmd

import (
	"errors"
	"fmt"
	"os"
	"pxf-cli/pxf"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

type ClusterData struct {
	Cluster    *cluster.Cluster
	Output     *cluster.RemoteOutput
	NumHosts   int
	connection *dbconn.DBConn
}

var (
	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Perform <command> on each segment host in the cluster",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the PXF server instances on master, standby master, and the segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			command := &pxf.InitCommand
			clusterData, err := doSetup(command)
			if err == nil {
				err = clusterRun(command, clusterData)
			}
			exitWithReturnCode(err)
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the PXF server instances on the segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			command := &pxf.StartCommand
			clusterData, err := doSetup(command)
			if err == nil {
				err = clusterRun(command, clusterData)
			}

			exitWithReturnCode(err)
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "Stop the PXF server instances on the segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			command := &pxf.StopCommand
			clusterData, err := doSetup(command)
			if err == nil {
				err = clusterRun(command, clusterData)
			}

			exitWithReturnCode(err)
		},
	}

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Sync PXF configs from master to standby master and the segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			command := &pxf.SyncCommand
			clusterData, err := doSetup(command)
			if err == nil {
				err = clusterRun(command, clusterData)
			}

			exitWithReturnCode(err)
		},
	}

	statusCmd = &cobra.Command{
		Use:   "status",
		Short: "Get status of PXF servers on the segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			command := &pxf.StatusCommand
			clusterData, err := doSetup(command)
			if err == nil {
				err = clusterRun(command, clusterData)
			}

			exitWithReturnCode(err)
		},
	}
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(statusCmd)
	clusterCmd.AddCommand(stopCmd)
	clusterCmd.AddCommand(syncCmd)
}

func exitWithReturnCode(err error) {
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func (c *ClusterData) CountHostsExcludingMaster() error {
	hostSegMap := make(map[string]int, 0)
	for contentID, seg := range c.Cluster.Segments {
		if contentID == -1 {
			continue
		}
		hostSegMap[seg.Hostname]++
	}
	c.NumHosts = len(hostSegMap)
	return nil
}

func GenerateStatusReport(command *pxf.Command, clusterData *ClusterData) string {
	cmdMsg := fmt.Sprintf(command.Messages(pxf.Status), clusterData.NumHosts)
	gplog.Info(cmdMsg)
	return cmdMsg
}

func GenerateOutput(command *pxf.Command, clusterData *ClusterData) error {
	numHosts := len(clusterData.Output.Stdouts)
	numErrors := clusterData.Output.NumErrors
	if numErrors == 0 {
		gplog.Info(command.Messages(pxf.Success), numHosts-numErrors, numHosts)
		return nil
	}
	response := ""
	for index, stderr := range clusterData.Output.Stderrs {
		if clusterData.Output.Errors[index] == nil {
			continue
		}
		host := clusterData.Cluster.Segments[index].Hostname
		errorMessage := stderr
		if len(errorMessage) == 0 {
			errorMessage = clusterData.Output.Stdouts[index]
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
	gplog.Info("ERROR: "+command.Messages(pxf.Error), numErrors, numHosts)
	gplog.Error("%s", response)
	return errors.New(response)
}

func doSetup(command *pxf.Command) (*ClusterData, error) {
	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err := connection.Connect(1)
	if err != nil {
		gplog.Error(fmt.Sprintf("ERROR: Could not connect to GPDB.\n%s\n"+
			"Please make sure that your Greenplum database is running and you are on the master node.", err.Error()))
		return nil, err
	}
	segConfigs := cluster.MustGetSegmentConfiguration(connection)
	clusterData := &ClusterData{Cluster: cluster.NewCluster(segConfigs), connection: connection}
	if command.Name() == pxf.Sync || command.Name() == pxf.Init {
		err = clusterData.appendMasterStandby()
		if err != nil {
			return nil, err
		}
	}
	err = clusterData.CountHostsExcludingMaster()
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}
	return clusterData, nil
}

func adaptContentIDToHostname(cluster *cluster.Cluster, f func(string) string) func(int) string {
	return func(contentId int) string {
		return f(cluster.GetHostForContent(contentId))
	}
}

func clusterRun(command *pxf.Command, clusterData *ClusterData) error {
	defer clusterData.connection.Close()
	f, err := command.GetFunctionToExecute()
	if err != nil {
		gplog.Error(fmt.Sprintf("Error: %s", err))
		return err
	}

	cmdMsg := GenerateStatusReport(command, clusterData)
	clusterData.Output = clusterData.Cluster.GenerateAndExecuteCommand(
		cmdMsg,
		adaptContentIDToHostname(clusterData.Cluster, f),
		command.WhereToRun(),
	)
	return GenerateOutput(command, clusterData)
}

func (c *ClusterData) appendMasterStandby() error {
	query := ""
	if c.connection.Version.Before("6") {
		query = `
SELECT
        s.dbid,
        s.content as contentid,
        s.port,
        s.hostname,
        e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'm' AND f.fsname = 'pg_system' AND s.content = '-1'
ORDER BY s.content;`
	} else {
		query = `
SELECT
        dbid,
        content as contentid,
        port,
        hostname,
        datadir
FROM gp_segment_configuration
WHERE role = 'm' AND content = '-1'
ORDER BY content;`
	}

	results := make([]cluster.SegConfig, 0)
	err := c.connection.Select(&results, query)
	if err != nil {
		return err
	}
	standbyMasterContentID := len(c.Cluster.Segments)
	if len(results) > 0 {
		c.Cluster.Segments[standbyMasterContentID] = results[0]
	}
	return nil
}
