package cmd

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

var (
	connectionPool *dbconn.DBConn
	globalCluster  *cluster.Cluster
	segConfigs     []cluster.SegConfig
)

func SetConnection(conn *dbconn.DBConn) {
	connectionPool = conn
}

func SetCluster(cluster *cluster.Cluster) {
	globalCluster = cluster
}

func SetSegConfigs(sCs []cluster.SegConfig) {
	segConfigs = sCs
}

func SetHostList(hostList map[string]int) {
	segHostList = hostList
}
