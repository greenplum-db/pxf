package main

import (
	"log"
	"os"
	"pxf-cluster/cluster"
	"pxf-cluster/pxf"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func main() {
	// InitializeLogging must be called before we attempt to log with gplog.
	gplog.InitializeLogging("pxf_cli", "")

	inputs, err := pxf.MakeValidCliInputs(os.Args)
	fatalOnError(err)

	cluster.RunCommand(inputs)
}

func fatalOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
