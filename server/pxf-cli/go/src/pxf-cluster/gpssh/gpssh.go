package gpssh

import "os/exec"

func Command(hosts []string, remoteCommand []string) *exec.Cmd {
	var args []string
	for _, host := range hosts {
		args = append(args, "-h")
		args = append(args, host)
	}
	args = append(args, remoteCommand...)

	return exec.Command("gpssh", args...)
}
