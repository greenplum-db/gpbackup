package utils_test

import (
	"strconv"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/plugin tests", func() {
	stdOut := make(map[int]string, 1)
	stdOut[0] = utils.RequiredPluginVersion // this is a successful result
	var testCluster *cluster.Cluster
	var executor testhelper.TestExecutor
	var subject utils.PluginConfig

	BeforeEach(func() {
		subject = utils.PluginConfig{
			ExecutablePath: "myPlugin",
		}
		executor = testhelper.TestExecutor{
			ClusterOutput: &cluster.RemoteOutput{
				Stdouts: stdOut,
			},
		}
		testCluster = &cluster.Cluster{
			ContentIDs: []int{-1, 0, 1},
			Executor:   &executor,
			Segments: map[int]cluster.SegConfig{
				-1: {DataDir: "/data/gpseg-1", Hostname: "master"},
				0:  {DataDir: "/data/gpseg0", Hostname: "segment1"},
				1:  {DataDir: "/data/gpseg1", Hostname: "segment2"},
			},
		}
	})
	AfterEach(func() {
		operating.System = operating.InitializeSystemFunctions()
	})
	Describe("gpbackup plugin interface generates the correct", func() {
		It("api command", func() {
			operating.System.Getenv = func(key string) string {
				return "my/install/dir"
			}

			subject.CheckPluginExistsOnAllHosts(testCluster)

			allCommands := executor.ClusterCommands[0] // only one set of commands was issued
			expectedCommand := "source my/install/dir/greenplum_path.sh && myPlugin plugin_api_version"
			for _, contentID := range testCluster.ContentIDs {
				cmd := allCommands[contentID]
				Expect(cmd[len(cmd)-1]).To(Equal(expectedCommand))
			}
		})
	})
	Describe("version validation", func() {
		When("version is equal to requirement", func() {
			It("succeeds", func() {
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version is greater than requirement", func() {
			It("succeeds", func() {
				// add one to whatever the current required version might be
				version, _ := semver.Make(utils.RequiredPluginVersion)
				greater, _ := semver.Make(strconv.Itoa(int(version.Major)+1) + ".0.0")
				executor.ClusterOutput.Stdouts[0] = greater.String()

				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version is too low", func() {
			It("panics with message", func() {
				executor.ClusterOutput.Stdouts[0] = "0.2.0"

				defer testhelper.ShouldPanicWithMessage("Plugin API version incorrect")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version cannot be parsed", func() {
			It("panics with message", func() {
				executor.ClusterOutput.Stdouts[0] = "foo"

				defer testhelper.ShouldPanicWithMessage("Unable to parse plugin API version")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version command fails", func() {
			It("panics with message", func() {
				subject.ExecutablePath = "myFailingPlugin"
				executor.ClusterOutput.NumErrors = 1

				defer testhelper.ShouldPanicWithMessage("Unable to execute plugin myFailingPlugin")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
	})
})
