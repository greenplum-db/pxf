package main

import (
	"fmt"
	"log"
	"os"
	"pxf-cluster/pxf"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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

	hostList := cluster.ON_HOSTS
	if inputs.Args[0] == "init" {
		hostList = cluster.ON_HOSTS_AND_MASTER
	}
	remoteOut := globalCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Executing command '%s' on all hosts", inputs.Args[0]),
		func(contentID int) string {
			return strings.Join(pxf.RemoteCommandToRunOnSegments(inputs), " ")
		},
		hostList)
	response := ""
	errCount := 0
	for index, error := range remoteOut.Stderrs {
		host := globalCluster.Segments[index].Hostname
		if len(error) > 0 {
			response += fmt.Sprintf("%s:\n%s\n", host, error)
			errCount++
			continue
		}
		fmt.Printf("Command '%s' completed successfully on %s.\n", inputs.Args[0], host)
	}
	if errCount == 0 {
		os.Exit(0)
	}
	response = fmt.Sprintf("Command '%s' failed on %d node[s]:\n\n", inputs.Args[0], errCount) + response
	fmt.Printf("%s", response)
	os.Exit(1)
}

func fatalOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
