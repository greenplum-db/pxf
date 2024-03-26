package config

import (
	"errors"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type PxfServiceGroup struct {
	Name  string
	Ports []int
	IsSsl bool
}

type PxfCluster struct {
	Name       string
	Collocated bool
	Hosts      []PxfHost
	Endpoint   string
	Groups     map[string]PxfServiceGroup
}

type PxfHost struct {
	Hostname string
}

type PxfDeployment struct {
	Name     string
	Clusters map[string]PxfCluster
}

/*

The model has implicit validation rules for the values of its entities attributes.

Violation of a "MUST" rule should result in a runtime ERROR. Violation of a "SHOULD" rule should result in a runtime WARNING.

Attribute values should be valid for their intended types (numbers, booleans, etc).
Cluster::name MUST be unique.
Cluster::collocated MUST not be NULL.
Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
Host::hostname MUST be unique (a single host must belong to only one cluster)
Host::hostname MUST have a value with either IPv4, IPv6 of FQDN compliant syntax.
ServiceGroup::name MUST NOT be null / empty.
ServiceGroup::name MUST be unique across all Clusters
ServiceGroup::ports MUST be a non-empty array with at least 1 element
ServiceGroup::ports elements MUST be an integer value between 1-65535
ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768 (between privileged and ephemeral ranges)
There MUST be at least one Cluster
There MUST be no more than one Cluster::collocated==true
There MUST be at least one ServiceGroup per Cluster

*/

func (p *PxfDeployment) Validate() error {
	set := make(map[string]bool)
	for _, pxfCluster := range p.Clusters {
		if _, ok := set[pxfCluster.Name]; ok {
			errMsg := fmt.Sprintf("ERROR: More than one cluster have the same name %s.", pxfCluster.Name)
			gplog.Error(errMsg)
			return errors.New(errMsg)
		}
	}
}

func (p *PxfCluster) Validate() error {

}

func (p *PxfServiceGroup) Validate() error {

}
