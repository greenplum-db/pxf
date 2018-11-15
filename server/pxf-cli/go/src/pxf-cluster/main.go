package main

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"log"
	"os"
	"pxf-cluster/pxf"
	"strings"
)

func main() {
	// InitializeLogging must be called before we attempt to log with gplog.
	gplog.InitializeLogging("pxf_cli", "")

	inputs, err := pxf.MakeValidCliInputs(os.Args)
	fatalOnError(err)

	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err = connection.Connect(1)
	if err != nil {
		os.Exit(1)
	}
	defer connection.Close()

	segConfigs := cluster.MustGetSegmentConfiguration(connection)

	globalCluster := cluster.NewCluster(segConfigs)
	var hostNames []string
	for _, config := range segConfigs {
		hostNames = append(hostNames, config.Hostname)
	}
	globalCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Executing %s on all hosts", inputs.Args[0]),
		func(contentID int) string {
		    return strings.Join(pxf.RemoteCommandToRunOnSegments(inputs), " ")
	    },
		cluster.ON_HOSTS_AND_MASTER)
}

func fatalOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
