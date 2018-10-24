package test

// ********************************************************
// NOTE: This file contains no tests.
// It just hooks Ginkgo into Go's built-in test framework.
// ********************************************************

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cluster tests")
}
