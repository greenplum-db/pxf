package pxf

import (
	"errors"
	"os"
)

type CliInputs struct {
	Gphome  string
	PxfConf string
	Args    []string
}

type EnvVar string

const (
	Gphome  EnvVar = "GPHOME"
	PxfConf EnvVar = "PXF_CONF"
)

func MakeValidCliInputs(args []string) (*CliInputs, error) {
	usageMessage := "usage: pxf cluster {start|stop|restart|init|status}"
	gphome, error := ValidateEnvVar(Gphome)
	if error != nil {
		return nil, error
	}
	if len(args) != 2 {
		return nil, errors.New(usageMessage)
	}
	switch args[1] {
	case "start", "stop", "restart", "status":
		return &CliInputs{Gphome: gphome, PxfConf: "", Args: args[1:]}, nil
	case "init":
		pxfConf, error := ValidateEnvVar(PxfConf)
		if error != nil {
			return nil, error
		}
		return &CliInputs{Gphome: gphome, PxfConf: pxfConf, Args: args[1:]}, nil
	}
	return nil, errors.New(usageMessage)
}

func ValidateEnvVar(envVar EnvVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVar))
	if !isEnvVarSet {
		return "", errors.New(string(envVar) + " must be set.")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVar) + " cannot be blank.")
	}
	return envVarValue, nil
}

func RemoteCommandToRunOnSegments(inputs *CliInputs) []string {
	pxfCommand := ""
	if inputs.PxfConf != "" {
		pxfCommand += "PXF_CONF=" + inputs.PxfConf + " "
	}
	pxfCommand += inputs.Gphome + "/pxf/bin/pxf"
	return []string{pxfCommand, inputs.Args[0]}
}
