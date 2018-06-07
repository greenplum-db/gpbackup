package utils_test

import (
	"os/user"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/compression tests", func() {
	masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
	})

	Describe("InitializeCompressionParameters", func() {
		It("initializes properly when passed no compression", func() {
			useCompress, compression := utils.GetCompressionParameters()
			defer utils.SetCompressionParameters(useCompress, compression)
			expectedCompress := utils.Compression{
				Name:              "gzip",
				CompressCommand:   "gzip -c -3",
				DecompressCommand: "gzip -d -c",
				Extension:         ".gz",
			}
			utils.InitializeCompressionParameters(false, 3)
			resultUseCompress, resultCompression := utils.GetCompressionParameters()
			Expect(resultUseCompress).To(BeFalse())
			structmatcher.ExpectStructsToMatch(&expectedCompress, &resultCompression)
		})
		It("initializes properly when passed compression", func() {
			useCompress, compression := utils.GetCompressionParameters()
			defer utils.SetCompressionParameters(useCompress, compression)
			expectedCompress := utils.Compression{
				Name:              "gzip",
				CompressCommand:   "gzip -c -7",
				DecompressCommand: "gzip -d -c",
				Extension:         ".gz",
			}
			utils.InitializeCompressionParameters(true, 7)
			resultUseCompress, resultCompression := utils.GetCompressionParameters()
			Expect(resultUseCompress).To(BeTrue())
			structmatcher.ExpectStructsToMatch(&expectedCompress, &resultCompression)
		})
		It("uses default gzip command when passed compression level 0", func() {
			useCompress, compression := utils.GetCompressionParameters()
			defer utils.SetCompressionParameters(useCompress, compression)
			expectedCompress := utils.Compression{
				Name:              "gzip",
				CompressCommand:   "gzip -c -1",
				DecompressCommand: "gzip -d -c",
				Extension:         ".gz",
			}
			utils.InitializeCompressionParameters(true, 0)
			resultUseCompress, resultCompression := utils.GetCompressionParameters()
			Expect(resultUseCompress).To(BeTrue())
			structmatcher.ExpectStructsToMatch(&expectedCompress, &resultCompression)
		})
	})
})
