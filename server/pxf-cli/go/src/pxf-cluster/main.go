package main

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"log"
	"os"
	"pxf-cluster/gpssh"
	"pxf-cluster/greenplum"
	"pxf-cluster/pxf"
)

func main() {
	// InitializeLogging must be called before we attempt to log with gplog.
	gplog.InitializeLogging("pxf_cli", "")

	inputs, err := pxf.MakeValidCliInputs(os.Args)
	fatalOnError(err)

	remoteCommand := remoteCommandToRunOnSegments(inputs)
	hosts, err := greenplum.GetSegmentHosts()
	fatalOnError(err)

	out, err := gpssh.Command(hosts, remoteCommand).CombinedOutput()
	fmt.Println(string(out))
	fatalOnError(err)
}

func fatalOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func remoteCommandToRunOnSegments(inputs *pxf.CliInputs) []string {
	return []string{inputs.Gphome + "/pxf/bin/pxf", inputs.Args[0]}
}