package config_test

import (
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"pxf-cli/config"
)

var _ = Describe("validate configuration for PXF deployment", func() {
	Context("deployment name is empty", func() {
		var pxfDeployment config.PxfDeployment
		BeforeEach(func() {
			pxfDeployment = config.PxfDeployment{
				Name:     "",
				Clusters: map[string]config.PxfCluster{},
			}
		})
		It("returns an error", func() {
			err := pxfDeployment.Validate()
			Expect(err).To(HaveOccurred())
			//fmt.Printf("%#v", err)
			Expect(err).To(Equal(errors.New("the name of the deployment cannot be empty string")))
		})
	})
})

var _ = Describe("validate configuration for PXF cluster", func() {
	Context("cluster name is empty", func() {
		BeforeEach(func() {

		})
		It("returns an", func() {

		})
	})
})

// host
// group
