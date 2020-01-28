package options_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)


func TestOptions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Options Suite")
}

var _ = BeforeSuite(func() {
	_, _, _ = testhelper.SetupTestLogger()
})
