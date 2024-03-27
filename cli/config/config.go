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
Violation of a "MUST" rule should result in a runtime ERROR. Violation of a "SHOULD" rule should result in a runtime WARNING.

Deployment level validations:
* Cluster::name MUST be unique.
Host::hostname MUST be unique (a single host must belong to only one cluster)
There MUST be at least one Cluster
There MUST be no more than one Cluster::collocated==true

Cluster level validations:
ServiceGroup::name MUST be unique across all Clusters
Cluster::collocated MUST not be NULL.
Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
Host::hostname MUST have a value with either IPv4, IPv6 of FQDN compliant syntax.
Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
There MUST be at least one ServiceGroup per Cluster

Service group level validations:
ServiceGroup::name MUST NOT be null / empty.
ServiceGroup::ports MUST be a non-empty array with at least 1 element
ServiceGroup::ports elements MUST be an integer value between 1-65535
ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768 (between privileged and ephemeral ranges)
*/

func (p *PxfDeployment) Validate() error {
	nameSet := make(map[string]bool)
	for _, pxfCluster := range p.Clusters {
		if _, ok := nameSet[pxfCluster.Name]; ok {
			errMsg := fmt.Sprintf("ERROR: Duplicate cluster name found in the 'config.txt': %s", pxfCluster.Name)
			gplog.Error(errMsg)
			return errors.New(errMsg)
		}
		nameSet[pxfCluster.Name] = true
		if err := pxfCluster.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (p *PxfCluster) Validate() error {
	return nil
}

func (p *PxfServiceGroup) Validate() error {
	return nil
}
