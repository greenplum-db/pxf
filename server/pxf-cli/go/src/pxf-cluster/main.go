package main

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"os"
	"pxf-cluster/gpssh"
	"pxf-cluster/greenplum"
)

func main() {
	// TODO: validate args; this panics if no args are passed.
	gplog.InitializeLogging("pxf", "")

	remoteCommand := []string{"/usr/local/greenplum-db-devel/pxf/bin/pxf", os.Args[1]}
	out, err := gpssh.Command(greenplum.GetSegmentHosts(), remoteCommand).CombinedOutput()

	fmt.Println(string(out))
	if err != nil {
		fmt.Println(err)
	}
}