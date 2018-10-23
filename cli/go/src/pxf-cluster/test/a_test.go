package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("running tests", func() {
	It("works", func() {
		Expect(1).To(Equal(1))
	})
})
