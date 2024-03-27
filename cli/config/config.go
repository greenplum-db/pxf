package config

import (
	"errors"
	"fmt"
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
	Name     string `validate:"nonzero"`
	Clusters map[string]PxfCluster
}

type PxfDeploymentValidator func(p PxfDeployment) error

// PxfDeploymentNameNotEmpty Deployment::name MUST NOT be null / empty.
func PxfDeploymentNameNotEmpty(p PxfDeployment) error {
	if p.Name == "" {
		return errors.New("the name of the deployment cannot be empty string")
	} else {
		return nil
	}
}

// PxfDeploymentAtLeastOneCluster There MUST be at least one Cluster
func PxfDeploymentAtLeastOneCluster(p PxfDeployment) error {
	if len(p.Clusters) == 0 {
		return fmt.Errorf("there should be at least one PXF cluster in PXF deployment `%s`", p.Name)
	} else {
		return nil
	}
}

// PxfDeploymentHostnameUnique Host::hostname MUST be unique (a single host must belong to only one cluster)
func PxfDeploymentHostnameUnique(p PxfDeployment) error {
	hostnameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, host := range cluster.Hosts {
			if _, ok := hostnameMap[host.Hostname]; ok {
				return fmt.Errorf("host `%s` must belong to only one pxf cluster across the pxf deplopyment",
					host.Hostname)
			}
			hostnameMap[host.Hostname] = true
		}
	}
	return nil
}

func PxfDeploymentGroupNameUnique(p PxfDeployment) error {
	groupNameMap := make(map[string]bool)
	for _, cluster := range p.Clusters {
		for _, group := range cluster.Groups {
			if _, ok := groupNameMap[group.Name]; ok {
				return fmt.Errorf("PXF group name `%s` must be unique across the pxf deplopyment",
					group.Name)
			}
			groupNameMap[group.Name] = true
		}
	}
	return nil
}

// PxfDeploymentNoMoreThanOneCollocated There MUST be no more than one Cluster::collocated==true
func PxfDeploymentNoMoreThanOneCollocated(p PxfDeployment) error {
	found := false
	for _, cluster := range p.Clusters {
		if cluster.Collocated {
			if found {
				return fmt.Errorf("there must be no more than one collocated PXF cluster")
			} else {
				found = true
			}
		}
	}
	return nil
}

// Validate on deployment level
// (DONE by map) Cluster::name MUST be unique.
// (DONE) Deployment::name MUST NOT be null / empty.
// (DONE) Host::hostname MUST be unique (a single host must belong to only one cluster)
// (DONE) There MUST be at least one Cluster
// (DONE) There MUST be no more than one Cluster::collocated==true
// (DONE) ServiceGroup::name MUST be unique across all Clusters
func (p PxfDeployment) Validate() error {
	var pxfDeploymentValidators = []PxfDeploymentValidator{
		PxfDeploymentNameNotEmpty,
		PxfDeploymentAtLeastOneCluster,
		PxfDeploymentHostnameUnique,
		PxfDeploymentNoMoreThanOneCollocated,
		PxfDeploymentGroupNameUnique,
	}
	for _, validator := range pxfDeploymentValidators {
		if err := validator(p); err != nil {
			return err
		}
	}
	// TODO: call validate() of PxfCluster
	return nil
}

// Validate on level validations:
// (DONE by bool) Cluster::collocated MUST not be NULL.
// TODO: Cluster::name MUST NOT be null / empty.
// TODO: Cluster::collocated==true SHOULD NOT have a list of hosts, they WILL be ignored.
// TODO: Cluster::collocated==true SHOULD NOT have a Cluster::endpoint, it WILL be ignored.
// TODO: Cluster::collocated==false AND Cluster::endpoint==NULL MUST have a non-empty list of hosts.
// TODO: Cluster::endpoint != NULL MUST have a value of a valid IPv4, IPv6 of FQDN compliant syntax.
// TODO: There MUST be at least one ServiceGroup per Cluster
func (p *PxfCluster) Validate() error {
	return nil
}

// Validate on PxfHost level:
// TODO: Host::hostname MUST have a value with either IPv4, IPv6 of FQDN compliant syntax.
func (p *PxfHost) Validate() error {
	return nil
}

// Validate on service group level:
// TODO: ServiceGroup::name MUST NOT be null / empty.
// TODO: ServiceGroup::ports MUST be a non-empty array with at least 1 element
// TODO: ServiceGroup::ports elements MUST be an integer value between 1-65535
// TODO: ServiceGroup::ports elements SHOULD be larger than 1023 and smaller than 32768 (between privileged and ephemeral ranges)
func (p *PxfServiceGroup) Validate() error {
	return nil
}
