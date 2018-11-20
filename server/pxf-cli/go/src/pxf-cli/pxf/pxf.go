package pxf

import (
	"errors"
	"os"
)

type CliInputs struct {
	Gphome  string
	PxfConf string
	Cmd     Command
}

type EnvVar string

const (
	Gphome  EnvVar = "GPHOME"
	PxfConf EnvVar = "PXF_CONF"
)

type Command string

const (
	Init  Command = "init"
	Start Command = "start"
	Stop  Command = "stop"
)

func MakeValidCliInputs(cmd Command) (*CliInputs, error) {
	gphome, error := ValidateEnvVar(Gphome)
	if error != nil {
		return nil, error
	}
	pxfConf := ""
	if cmd == Init {
		pxfConf, error = ValidateEnvVar(PxfConf)
		if error != nil {
			return nil, error
		}
	}
	return &CliInputs{Cmd: cmd, Gphome: gphome, PxfConf: pxfConf}, nil
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

func RemoteCommandToRunOnSegments(inputs *CliInputs) string {
	pxfCommand := ""
	if inputs.PxfConf != "" {
		pxfCommand += "PXF_CONF=" + inputs.PxfConf + " "
	}
	pxfCommand += inputs.Gphome + "/pxf/bin/pxf" + " " + string(inputs.Cmd)
	return pxfCommand
}
