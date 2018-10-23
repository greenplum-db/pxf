package a_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cluster tests")
}

var _ = Describe("running tests", func() {
	It("works", func() {
		Expect(1).To(Equal(1))
	})
})
