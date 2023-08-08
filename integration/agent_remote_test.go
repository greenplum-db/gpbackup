package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("agent remote", func() {
	var (
		oidList     []string
		filePath    filepath.FilePathInfo
		testCluster *cluster.Cluster
	)
	BeforeEach(func() {
		oidList = []string{"1", "2", "3"}
		segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
		testCluster = cluster.NewCluster(segConfig)
		filePath = filepath.NewFilePathInfo(testCluster, "my_dir", "20190102030405", "")
	})
	Describe("WriteOidListToSegments()", func() {
		It("writes oids to a temp file and copies it to all segments", func() {
			utils.WriteOidListToSegments(oidList, testCluster, filePath, "oid")

			remoteOutput := testCluster.GenerateAndExecuteCommand("ensure oid file was written to segments", cluster.ON_SEGMENTS, func(contentID int) string {
				remoteOidFile := filePath.GetSegmentHelperFilePath(contentID, "oid")
				return fmt.Sprintf("cat %s", remoteOidFile)
			})
			defer func() {
				remoteOutputRemoval := testCluster.GenerateAndExecuteCommand("ensure oid file removed", cluster.ON_SEGMENTS, func(contentID int) string {
					remoteOidFile := filePath.GetSegmentHelperFilePath(contentID, "oid")
					return fmt.Sprintf("rm %s", remoteOidFile)
				})
				testCluster.CheckClusterError(remoteOutputRemoval, "Could not remove oid file", func(contentID int) string {
					return "Could not remove oid file"
				})
			}()

			testCluster.CheckClusterError(remoteOutput, "Could not cat oid file", func(contentID int) string {
				return "Could not cat oid file"
			})

			for _, cmd := range remoteOutput.Commands {
				Expect(cmd.Stdout).To(Equal("1\n2\n3\n"))
			}
		})
	})
})
