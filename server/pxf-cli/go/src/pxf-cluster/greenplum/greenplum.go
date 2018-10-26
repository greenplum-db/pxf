package greenplum

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

type HostnameRow = struct {
	Hostname string
}

func GetSegmentHosts() ([]string, error) {
	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err := connection.Connect(1)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	outputRows := make([]HostnameRow, 0)

	err = connection.Select(&outputRows, "select distinct hostname from gp_segment_configuration where content != -1")
	if err != nil {
		return nil, err
	}

	return HostnameFields(outputRows), nil
}

func HostnameFields(rows []HostnameRow) []string {
	var output []string
	for _, row := range rows {
		output = append(output, row.Hostname)
	}
	return output
}
