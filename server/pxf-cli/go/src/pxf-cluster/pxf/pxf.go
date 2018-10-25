package pxf

import (
	"errors"
	"os"
)

type CliInputs struct {
	Gphome string
	Args []string
}

func MakeValidCliInputs(args []string) (error, *CliInputs) {
	gphome, isGphomeSet := os.LookupEnv("GPHOME")
	if !isGphomeSet {
		return errors.New("GPHOME is not set"), nil
	}
	if gphome == "" {
		return errors.New("GPHOME is blank"), nil
	}
	if len(args) != 2 {
		return errors.New("Usage: pxf cluster {start|stop|restart|init|status}"), nil
	}

	return nil, &CliInputs{
		Gphome: os.Getenv("GPHOME"),
		Args: args[1:],
	}
}
//
//func CommandForSegments(getEnv func(string) string, args []string) (error, []string) {
//	return nil, nil
//}
//
//pxf.CliLocation(os.GetEnv)
//pxf.GetArgs(os.Args)
//
//pxf.CommandForSegment(os.getEnv, os.Args) -> /usr/local/greenplum/pxf/bin/pxf init