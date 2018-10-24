package test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"pxf-cluster/greenplum"
)

var _ = Describe("greenplum", func() {
	It("gets the host name fields as a string array", func() {

		rows := []greenplum.HostnameRow {
			greenplum.HostnameRow{"sdw1"},
			greenplum.HostnameRow{"sdw2"},
		}

		outputRows := greenplum.HostnameFields(rows)

		Expect(outputRows).To(Equal([]string{"sdw1", "sdw2"}))
	})
})

