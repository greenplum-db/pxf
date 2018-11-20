package cluster

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"os"
	"pxf-cluster/pxf"
	"strings"
)

const catalinaError = "SEVERE: No shutdown port configured"

type Command string

const (
	Init    Command = "init"
	Start   Command = "start"
	Stop    Command = "stop"
)

var successMessage = map[Command]string {
	Init: "PXF initialized successfully on %d out of %d nodes\n",
	Start: "PXF started successfully on %d out of %d nodes\n",
	Stop: "PXF stopped successfully on %d out of %d nodes\n",
}

var errorMessage = map[Command]string {
	Init: "PXF failed to initialize on %d out of %d nodes\n",
	Start: "PXF failed to start on %d out of %d nodes\n",
	Stop: "PXF failed to stop on %d out of %d nodes\n",
}

func RunCommand(inputs *pxf.CliInputs) {
	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err := connection.Connect(1)
	if err != nil {
		os.Exit(1)
	}
	defer connection.Close()

	segConfigs := cluster.MustGetSegmentConfiguration(connection)

	globalCluster := cluster.NewCluster(segConfigs)

	hostList := cluster.ON_HOSTS
	command := Command(inputs.Args[0])
	if command == Init {
		hostList = cluster.ON_HOSTS_AND_MASTER
	}
	remoteOut := globalCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Executing command '%s' on all hosts", command),
		func(contentID int) string {
			return strings.Join(pxf.RemoteCommandToRunOnSegments(inputs), " ")
		},
		hostList)
	response := ""
	errCount := 0
	numHosts := len(remoteOut.Stderrs)
	for index, error := range remoteOut.Stderrs {
		host := globalCluster.Segments[index].Hostname
		if len(error) > 0 {
			if command == Stop && strings.Contains(error, catalinaError) {
				continue
			}
			response += fmt.Sprintf("%s ==> %s\n", host, error)
			errCount++
			continue
		}
	}
	if errCount == 0 {
		fmt.Printf(successMessage[command], numHosts - errCount, numHosts)
		os.Exit(0)
	}
	fmt.Printf("ERROR: " + errorMessage[command], errCount, numHosts)
	fmt.Printf("%s", response)
	os.Exit(1)
}
