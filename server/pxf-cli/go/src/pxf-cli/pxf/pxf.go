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

var (
	SuccessMessage = map[Command]string{
		Init:  "PXF initialized successfully on %d out of %d nodes\n",
		Start: "PXF started successfully on %d out of %d nodes\n",
		Stop:  "PXF stopped successfully on %d out of %d nodes\n",
	}

	ErrorMessage = map[Command]string{
		Init:  "PXF failed to initialize on %d out of %d nodes\n",
		Start: "PXF failed to start on %d out of %d nodes\n",
		Stop:  "PXF failed to stop on %d out of %d nodes\n",
	}
)

func makeValidCliInputs(cmd Command) (*CliInputs, error) {
	gphome, error := validateEnvVar(Gphome)
	if error != nil {
		return nil, error
	}
	pxfConf := ""
	if cmd == Init {
		pxfConf, error = validateEnvVar(PxfConf)
		if error != nil {
			return nil, error
		}
	}
	return &CliInputs{Cmd: cmd, Gphome: gphome, PxfConf: pxfConf}, nil
}

func validateEnvVar(envVar EnvVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVar))
	if !isEnvVarSet {
		return "", errors.New(string(envVar) + " must be set")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVar) + " cannot be blank")
	}
	return envVarValue, nil
}

func RemoteCommandToRunOnSegments(cmd Command) (string, error) {
	inputs, err := makeValidCliInputs(cmd)
	if err != nil {
		return "", err
	}
	pxfCommand := ""
	if inputs.PxfConf != "" {
		pxfCommand += "PXF_CONF=" + inputs.PxfConf + " "
	}
	pxfCommand += inputs.Gphome + "/pxf/bin/pxf" + " " + string(inputs.Cmd)
	return pxfCommand, nil
}
