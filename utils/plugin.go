package utils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"gopkg.in/yaml.v2"
)

type PluginConfig struct {
	ExecutablePath string
	ConfigPath     string
	Options        map[string]string
}

type PluginScope string

const (
	Master      PluginScope = "master"
	SegmentHost PluginScope = "segment_host"
	Segment     PluginScope = "segment"
)

func ReadPluginConfig(configFile string) *PluginConfig {
	config := &PluginConfig{}
	contents, err := operating.System.ReadFile(configFile)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, config)
	gplog.FatalOnError(err)
	config.ExecutablePath = os.ExpandEnv(config.ExecutablePath)
	ValidateFullPath(config.ExecutablePath)
	_, configFilename := filepath.Split(configFile)
	config.ConfigPath = filepath.Join("/tmp", configFilename)
	return config
}

func (plugin *PluginConfig) BackupFile(filenamePath string, noFatal ...bool) {
	command := fmt.Sprintf("%s backup_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, filenamePath)
	output, err := exec.Command("bash", "-c", command).CombinedOutput()
	if err != nil {
		if len(noFatal) == 1 && noFatal[0] == true {
			gplog.Error(fmt.Sprintf("Plugin failed to process %s. %s", filenamePath, string(output)))
		} else {
			gplog.Fatal(err, string(output))
		}
	}
	err = operating.System.Chmod(filenamePath, 0755)
	gplog.FatalOnError(err)
}

func (plugin *PluginConfig) RestoreFile(filenamePath string) {
	directory, _ := filepath.Split(filenamePath)
	err := operating.System.MkdirAll(directory, 0755)
	gplog.FatalOnError(err)
	command := fmt.Sprintf("%s restore_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, filenamePath)
	output, err := exec.Command("bash", "-c", command).CombinedOutput()
	gplog.FatalOnError(err, string(output))
}

func (plugin *PluginConfig) CheckPluginExistsOnAllHosts(c *cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Checking that plugin exists on all hosts", func(contentID int) string {
		return fmt.Sprintf("source %s/greenplum_path.sh && %s plugin_api_version", operating.System.Getenv("GPHOME"), plugin.ExecutablePath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		supportedVersion, _ := semver.Make("0.2.0")
		version, err := semver.Make(strings.TrimSpace(remoteOutput.Stdouts[contentID]))
		if err != nil {
			gplog.Fatal(fmt.Errorf("Unable to parse plugin API version: %s", err.Error()), "")
		}
		if !version.Equals(supportedVersion) {
			gplog.Verbose("Plugin %s API version %s is not compatibile with supported API version %s", plugin.ExecutablePath, version, supportedVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("Plugin API version incorrect", cluster.ON_HOSTS_AND_MASTER, numIncorrect)
	}
}

/*-----------------------------Hooks------------------------------------------*/

func (plugin *PluginConfig) SetupPluginForBackup(c *cluster.Cluster, configPath string, backupDir string) {
	const command = "setup_plugin_for_backup"
	const verboseCommandMsg = "Running plugin setup for backup on %s"
	plugin.executeHook(c, verboseCommandMsg, command, configPath, backupDir, false)
}

func (plugin *PluginConfig) SetupPluginForRestore(c *cluster.Cluster, configPath string, backupDir string) {
	const command = "setup_plugin_for_restore"
	const verboseCommandMsg = "Running plugin setup for restore on %s"
	plugin.executeHook(c, verboseCommandMsg, command, configPath, backupDir, false)
}

func (plugin *PluginConfig) CleanupPluginForBackup(c *cluster.Cluster, configPath string, backupDir string) {
	const command = "cleanup_plugin_for_backup"
	const verboseCommandMsg = "Running plugin cleanup for backup on %s"
	plugin.executeHook(c, verboseCommandMsg, command, configPath, backupDir, true)
}

func (plugin *PluginConfig) CleanupPluginForRestore(c *cluster.Cluster, configPath string, backupDir string) {
	const command = "cleanup_plugin_for_restore"
	const verboseCommandMsg = "Running plugin cleanup for restore on %s"
	plugin.executeHook(c, verboseCommandMsg, command, configPath, backupDir, true)
}

func (plugin *PluginConfig) executeHook(c *cluster.Cluster, verboseCommandMsg,
	command, configPath, backupDir string, noFatal bool) {
	// Execute command once on master
	scope := Master
	hookFunc := plugin.buildHookFunc(command, configPath, backupDir, scope)
	verboseErrorMsg, errorMsgFunc := plugin.buildHookErrorMsgAndFunc(command, scope)
	masterOutput, masterErr := c.ExecuteLocalCommand(plugin.buildHookString(command,
		configPath, backupDir, scope))
	gplog.FatalOnError(masterErr, masterOutput)

	// Execute command once on each segment host
	scope = SegmentHost
	hookFunc = plugin.buildHookFunc(command, configPath, backupDir, scope)
	verboseErrorMsg, errorMsgFunc = plugin.buildHookErrorMsgAndFunc(command, scope)
	verboseCommandHostMasterMsg := fmt.Sprintf(verboseCommandMsg, "segment hosts")
	remoteOutput := c.GenerateAndExecuteCommand(verboseCommandHostMasterMsg, hookFunc, cluster.ON_HOSTS)
	c.CheckClusterError(remoteOutput, verboseErrorMsg, errorMsgFunc, noFatal)

	// Execute command once for each segment
	scope = Segment
	hookFunc = plugin.buildHookFunc(command, configPath, backupDir, scope)
	verboseErrorMsg, errorMsgFunc = plugin.buildHookErrorMsgAndFunc(command, scope)
	verboseCommandSegMsg := fmt.Sprintf(verboseCommandMsg, "segments")
	remoteOutput = c.GenerateAndExecuteCommand(verboseCommandSegMsg, hookFunc, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, verboseErrorMsg, errorMsgFunc, noFatal)
}

func (plugin *PluginConfig) buildHookFunc(command, configPath, backupDir string,
	scope PluginScope) func(int) string {
	return func(contentID int) string {
		return plugin.buildHookString(command, configPath, backupDir, scope)
	}
}

func (plugin *PluginConfig) buildHookString(command, configPath,
	backupDir string, scope PluginScope) string {
	return fmt.Sprintf("source %s/greenplum_path.sh && %s %s %s %s %s",
		operating.System.Getenv("GPHOME"), plugin.ExecutablePath, command, configPath, backupDir, scope)
}

func (plugin *PluginConfig) buildHookErrorMsgAndFunc(command string,
	scope PluginScope) (string, func(int) string) {
	errorMsg := fmt.Sprintf("Unable to execute command: %s at: %s, on: %s",
		command, plugin.ExecutablePath, scope)
	return errorMsg, func(contentID int) string {
		return errorMsg
	}
}

/*---------------------------------------------------------------------------------------------------*/

func (plugin *PluginConfig) CopyPluginConfigToAllHosts(c *cluster.Cluster, configPath string) {
	remoteOutput := c.GenerateAndExecuteCommand("Copying plugin config to all hosts", func(contentID int) string {
		return fmt.Sprintf("rsync %s:%s /tmp/.", c.GetHostForContent(-1), configPath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, "Unable to copy plugin config", func(contentID int) string {
		return "Unable to copy plugin config"
	})
}

func (plugin *PluginConfig) BackupSegmentTOCs(c *cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Checking that TOC file exists", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		return fmt.Sprintf(`while [[ ! -f "%s" && ! -f "%s" ]]; do sleep 1; done; ls "%s"`, tocFile, errorFile, tocFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Error occurred in gpbackup_helper", func(contentID int) string {
		return "See gpAdminLog for gpbackup_helper on segment host for details: Error occurred with plugin"
	})

	remoteOutput = c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		return fmt.Sprintf("source %s/greenplum_path.sh && %s backup_file %s %s && chmod 0755 %s", operating.System.Getenv("GPHOME"), plugin.ExecutablePath, plugin.ConfigPath, tocFile, tocFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return "See gpAdminLog for gpbackup_helper on segment host for details: Error occurred with plugin"
	})
}

func (plugin *PluginConfig) RestoreSegmentTOCs(c *cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		return fmt.Sprintf("mkdir -p %s && source %s/greenplum_path.sh && %s restore_file %s %s", fpInfo.GetDirForContent(contentID), operating.System.Getenv("GPHOME"), plugin.ExecutablePath, plugin.ConfigPath, tocFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}
