package pxf

import (
	"errors"
	"os"
)

type CliInputs struct {
	Gphome string
	Args   []string
}

func MakeValidCliInputs(args []string) (*CliInputs, error) {
	usageMessage := "usage: pxf cluster {start|stop|restart|init|status}"
	gphome, isGphomeSet := os.LookupEnv("GPHOME")
	if !isGphomeSet {
		return nil, errors.New("GPHOME is not set")
	}
	if gphome == "" {
		return nil, errors.New("GPHOME is blank")
	}
	if len(args) != 2 {
		return nil, errors.New(usageMessage)
	}
	switch args[1] {
	case "init", "start", "stop", "restart", "status":
		return &CliInputs{
			Gphome: os.Getenv("GPHOME"),
			Args:   args[1:],
		}, nil
	}
	return nil, errors.New(usageMessage)
}

func RemoteCommandToRunOnSegments(inputs *CliInputs) []string {
	return []string{inputs.Gphome + "/pxf/bin/pxf", inputs.Args[0]}
}
