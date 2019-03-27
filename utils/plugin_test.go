package utils_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/greenplum-db/gpbackup/testutils"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/pkg/errors"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/plugin tests", func() {
	var testCluster *cluster.Cluster
	var executor testutils.TestExecutorMultiple
	var subject utils.PluginConfig
	var tempDir string

	BeforeEach(func() {
		tempDir, _ = ioutil.TempDir("", "temp")
		operating.System.Stdout = stdout
		subject = utils.PluginConfig{
			ExecutablePath: "/a/b/myPlugin",
			ConfigPath:     "/tmp/my_plugin_config.yaml",
			Options:        make(map[string]string),
		}
		subject.Options = make(map[string]string, 0)
		executor = testutils.TestExecutorMultiple{
			ClusterOutputs: make([]*cluster.RemoteOutput, 2),
		}
		// set up fake command results
		apiResponse := make(map[int]string, 1)
		apiResponse[-1] = utils.RequiredPluginVersion // this is a successful result fpr API version
		apiResponse[0] = utils.RequiredPluginVersion
		apiResponse[1] = utils.RequiredPluginVersion
		executor.ClusterOutputs[0] = &cluster.RemoteOutput{
			Stdouts: apiResponse,
		}
		nativeResponse := make(map[int]string, 1)
		nativeResponse[-1] = "myPlugin version 1.2.3" // this is a successful result for --version
		nativeResponse[0] = "myPlugin version 1.2.3"
		nativeResponse[1] = "myPlugin version 1.2.3"
		executor.ClusterOutputs[1] = &cluster.RemoteOutput{
			Stdouts: nativeResponse,
		}

		testCluster = &cluster.Cluster{
			ContentIDs: []int{-1, 0, 1},
			Executor:   &executor,
			Segments: map[int]cluster.SegConfig{
				-1: {DataDir: filepath.Join(tempDir, "seg-1"), Hostname: "master", Port: 100},
				0:  {DataDir: filepath.Join(tempDir, "seg0"), Hostname: "segment1", Port: 101},
				1:  {DataDir: filepath.Join(tempDir, "seg1"), Hostname: "segment2", Port: 102},
			},
		}
	})
	AfterEach(func() {
		err := os.RemoveAll(tempDir)
		Expect(err).To(Not(HaveOccurred()))
		_ = os.Remove(subject.ConfigPath)
		_ = os.Remove(subject.ConfigPath + "_-1")
		_ = os.Remove(subject.ConfigPath + "_0")
		_ = os.Remove(subject.ConfigPath + "_1")
	})
	Describe("plugin versions via CheckPluginExistsOnAllHosts()", func() {
		It(" generates the correct commands", func() {
			operating.System.Getenv = func(key string) string {
				return "my/install/dir"
			}

			_ = subject.CheckPluginExistsOnAllHosts(testCluster)

			apiVersionCommands := executor.ClusterCommands[0]
			expectedCommand := "source my/install/dir/greenplum_path.sh && /a/b/myPlugin plugin_api_version"
			for _, contentID := range testCluster.ContentIDs {
				cmd := apiVersionCommands[contentID]
				Expect(cmd[len(cmd)-1]).To(Equal(expectedCommand))
			}
			nativeVersionCommands := executor.ClusterCommands[1]
			expectedCommand = "source my/install/dir/greenplum_path.sh && /a/b/myPlugin --version"
			for _, contentID := range testCluster.ContentIDs {
				cmd := nativeVersionCommands[contentID]
				Expect(cmd[len(cmd)-1]).To(Equal(expectedCommand))
			}
		})
	})
	Describe("copy plugin config", func() {
		It("successfully copies to all hosts, appending PGPORT and the --version of the plugin", func() {
			testConfigPath := "/tmp/my_plugin_config.yaml"
			testConfigContents := `
executablepath: /tmp/fake_path
options:
    field1: 12
    field2: hello
    field3: 567
`
			err := ioutil.WriteFile(testConfigPath, []byte(testConfigContents), 0777)
			Expect(err).To(Not(HaveOccurred()))
			subject.SetBackupPluginVersion("myTimestamp", "my.test.version")
			subject.CopyPluginConfigToAllHosts(testCluster)

			Expect(executor.NumRemoteExecutions).To(Equal(1))
			cc := executor.ClusterCommands[0]
			Expect(len(cc)).To(Equal(3))
			Expect(cc[-1][2]).To(Equal("scp /tmp/my_plugin_config.yaml_-1 master:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_-1"))
			Expect(cc[0][2]).To(Equal("scp /tmp/my_plugin_config.yaml_0 segment1:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_0"))
			Expect(cc[1][2]).To(Equal("scp /tmp/my_plugin_config.yaml_1 segment2:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_1"))

			// check contents
			contents := strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_-1"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"100\""))
			Expect(contents).To(ContainSubstring("\n  backup_plugin_version: my.test.version"))
			contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_0"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"101\""))
			Expect(contents).To(ContainSubstring("\n  backup_plugin_version: my.test.version"))
			contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_1"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"102\""))
			Expect(contents).To(ContainSubstring("\n  backup_plugin_version: my.test.version"))
		})
		When("copying for a plugin with encryption", func() {
			It("copies the encryption key", func() {
				executor.LocalOutput = "gpbackup_fake_plugin version 1.0.1+dev.28.g00c877e"
				testConfigPath := "/tmp/my_plugin_config.yaml"
				testConfigContents := `
executablepath: /tmp/foobar
options:
    field1: 12
    field2: hello
    field3: 567
`
				err := ioutil.WriteFile(testConfigPath, []byte(testConfigContents), 0777)
				subject.Options["password_encryption"] = "on"
				mdd := testCluster.GetDirForContent(-1)
				_ = os.MkdirAll(mdd, 0777)
				secretFilePath := filepath.Join(mdd, utils.SecretKeyFile)
				secretFile := iohelper.MustOpenFileForWriting(secretFilePath)
				_, err = secretFile.Write([]byte(`gpbackup_fake_plugin: 0123456789`))
				Expect(err).To(Not(HaveOccurred()))

				subject.CopyPluginConfigToAllHosts(testCluster)

				// check contents
				contents := strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_-1"), "\n")
				Expect(contents).To(ContainSubstring("\n  gpbackup_fake_plugin: \"0123456789\""))
				contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_0"), "\n")
				Expect(contents).To(ContainSubstring("\n  gpbackup_fake_plugin: \"0123456789\""))
				contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_1"), "\n")
				Expect(contents).To(ContainSubstring("\n  gpbackup_fake_plugin: \"0123456789\""))
			})
			It("writes a stdout message when encrypt key is not found", func() {
				subject.Options["password_encryption"] = "on"
				executor.LocalOutput = "gpbackup_fake_plugin version 1.0.1+dev.28.g00c877e"
				pluginName, err := subject.GetPluginName(testCluster)
				Expect(err).To(Not(HaveOccurred()))
				errMsg := fmt.Sprintf("Cannot find encryption key for plugin %s. Please re-encrypt password(s) so that key becomes available.", pluginName)
				defer testhelper.ShouldPanicWithMessage(errMsg)
				subject.CopyPluginConfigToAllHosts(testCluster)

				Expect(string(stdout.Contents())).To(ContainSubstring(errMsg))
				Expect(string(stdout.Contents())).To(ContainSubstring(errMsg))
			})
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
				executor.ClusterOutputs[0].Stdouts[-1] = greater.String()
				executor.ClusterOutputs[0].Stdouts[0] = greater.String()
				executor.ClusterOutputs[0].Stdouts[1] = greater.String()

				_ = subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version is too low", func() {
			It("panics with message", func() {
				executor.ClusterOutputs[0].Stdouts[-1] = "0.2.0"
				executor.ClusterOutputs[0].Stdouts[0] = "0.2.0"
				executor.ClusterOutputs[0].Stdouts[1] = "0.2.0"
				defer testhelper.ShouldPanicWithMessage("Plugin API version incorrect")

				_ = subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version cannot be parsed", func() {
			It("panics with message", func() {
				executor.ClusterOutputs[0].Stdouts[-1] = "foo"
				executor.ClusterOutputs[0].Stdouts[0] = "foo"
				executor.ClusterOutputs[0].Stdouts[1] = "foo"
				defer testhelper.ShouldPanicWithMessage("Unable to parse plugin API version")

				_ = subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version command fails", func() {
			It("panics with message", func() {
				subject.ExecutablePath = "myFailingPlugin"
				executor.ClusterOutputs[0].NumErrors = 1
				defer testhelper.ShouldPanicWithMessage("Unable to execute plugin myFailingPlugin")

				_ = subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version inconsistent", func() {
			It("panics with message", func() {
				executor.ClusterOutputs[0].Stdouts[-1] = "99.99.9999"
				defer testhelper.ShouldPanicWithMessage("Plugin API version is inconsistent across segments")

				_ = subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
	})
	Describe("UsesEncryption", func() {
		It("returns false when there is no encryption in config", func() {
			Expect(subject.UsesEncryption()).To(BeFalse())
		})
		It("returns true when there is local encryption in config", func() {
			subject.Options["password_encryption"] = "on"
			Expect(subject.UsesEncryption()).To(BeTrue())
		})
		It("returns true when there is remote encryption in config", func() {
			subject.Options["replication"] = "on"
			subject.Options["remote_password_encryption"] = "on"
			Expect(subject.UsesEncryption()).To(BeTrue())
		})
	})
	Describe("GetSecretKey", func() {
		It("returns a secret key when one exists for the given name", func() {
			mdd := testCluster.GetDirForContent(-1)
			_ = os.MkdirAll(mdd, 0777)
			secretFilePath := filepath.Join(mdd, utils.SecretKeyFile)
			err := ioutil.WriteFile(secretFilePath, []byte(`gpbackup_fake_plugin: 0123456789`), 0777)
			Expect(err).To(Not(HaveOccurred()))

			key, err := utils.GetSecretKey("gpbackup_fake_plugin", mdd)

			Expect(err).To(Not(HaveOccurred()))
			Expect(key).To(Equal("0123456789"))
		})
		It("returns an error when no encrypt file exists for the given name", func() {
			mdd := testCluster.GetDirForContent(-1)

			pluginName := "gpbackup_fake_plugin"
			_, err := utils.GetSecretKey(pluginName, mdd)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(fmt.Sprintf("Cannot find encryption key for plugin %s. Please re-encrypt password(s) so that key becomes available.", pluginName)))
		})
		It("returns an error when no key exists for the given name", func() {
			mdd := testCluster.GetDirForContent(-1)
			_ = os.MkdirAll(mdd, 0777)
			secretFilePath := filepath.Join(mdd, utils.SecretKeyFile)
			err := ioutil.WriteFile(secretFilePath, []byte(""), 0777)
			Expect(err).To(Not(HaveOccurred()))

			pluginName := "gpbackup_fake_plugin"
			_, err = utils.GetSecretKey(pluginName, mdd)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(fmt.Sprintf("Cannot find encryption key for plugin %s. Please re-encrypt password(s) so that key becomes available.", pluginName)))
		})
		It("returns an error when encrypt file cannot be parsed", func() {
			mdd := testCluster.GetDirForContent(-1)
			_ = os.MkdirAll(mdd, 0777)
			secretFilePath := filepath.Join(mdd, utils.SecretKeyFile)
			err := ioutil.WriteFile(secretFilePath, []byte("improperlyFormattedYaml"), 0777)
			Expect(err).To(Not(HaveOccurred()))

			pluginName := "gpbackup_fake_plugin"
			_, err = utils.GetSecretKey(pluginName, mdd)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(fmt.Sprintf("Cannot find encryption key for plugin %s. Please re-encrypt password(s) so that key becomes available.", pluginName)))
		})
	})
	Describe("DeleteConfigFileOnSegments", func() {
		When("config has encryption", func() {
			It("sends the correct cluster command to delete config file", func() {
				subject.Options["password_encryption"] = "on"

				subject.DeletePluginConfigWhenEncrypting(testCluster)

				Expect(executor.NumRemoteExecutions).To(Equal(1))
				cc := executor.ClusterCommands[0]
				Expect(len(cc)).To(Equal(3))
				Expect(cc[-1][2]).To(Equal("rm -f /tmp/my_plugin_config.yaml"))
				Expect(cc[0][2]).To(Equal("rm -f /tmp/my_plugin_config.yaml"))
				Expect(cc[1][2]).To(Equal("rm -f /tmp/my_plugin_config.yaml"))
			})
		})
		When("config does not have encryption", func() {
			It("does not send a cluster command to delete config file", func() {
				subject.DeletePluginConfigWhenEncrypting(testCluster)

				Expect(executor.NumLocalExecutions).To(Equal(0))
			})
		})
	})
	Describe("GetPluginName", func() {
		It("make the correct plugin call, parses out plugin name correctly, and returns it", func() {
			executor.LocalOutput = "gpbackup_fake_plugin version 1.0.1+dev.28.g00c877e"
			pluginName, err := subject.GetPluginName(testCluster)

			Expect(err).To(Not(HaveOccurred()))
			Expect(executor.LocalCommands[0]).To(Equal("/a/b/myPlugin --version"))
			Expect(pluginName).To(Equal("gpbackup_fake_plugin"))
		})
		It("encountered an error running plugin command", func() {
			executor.LocalError = errors.New("error executing plugin")
			pluginName, err := subject.GetPluginName(testCluster)

			Expect(pluginName).To(Equal(""))
			Expect(err.Error()).To(Equal("Failed to get plugin name. Failed with error: error executing plugin"))
		})
		It("did not recieve expected information from plugin", func() {
			executor.LocalOutput = "bad output"
			pluginName, err := subject.GetPluginName(testCluster)

			Expect(pluginName).To(Equal(""))
			Expect(err.Error()).To(Equal("Unexpected plugin version format: \"bad output\"\nExpected: \"[plugin_name] version [git_version]\""))
		})
	})
})
