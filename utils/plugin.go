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
	yaml "gopkg.in/yaml.v2"
)

type PluginConfig struct {
	ExecutablePath string
	ConfigPath     string
	Options        map[string]string
}

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
}

func (plugin *PluginConfig) RestoreFile(filenamePath string) {
	command := fmt.Sprintf("%s restore_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, filenamePath)
	output, err := exec.Command("bash", "-c", command).CombinedOutput()
	gplog.FatalOnError(err, string(output))
}

func (plugin *PluginConfig) CheckPluginExistsOnAllHosts(c cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Checking that plugin exists on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s plugin_api_version", plugin.ExecutablePath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		supportedVersion, _ := semver.Make("0.1.0")
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

func (plugin *PluginConfig) SetupPluginForBackupOnAllHosts(c cluster.Cluster, configPath string, backupDir string) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin setup for backup on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s setup_plugin_for_backup %s %s", plugin.ExecutablePath, configPath, backupDir)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) SetupPluginForRestoreOnAllHosts(c cluster.Cluster, configPath string, backupDir string) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin setup for restore on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s setup_plugin_for_restore %s %s", plugin.ExecutablePath, configPath, backupDir)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) CleanupPluginForBackupOnAllHosts(c cluster.Cluster, configPath string, backupDir string) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin cleanup for backup on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s cleanup_plugin_for_backup %s %s", plugin.ExecutablePath, configPath, backupDir)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath)
	}, true)
}

func (plugin *PluginConfig) CleanupPluginForRestoreOnAllHosts(c cluster.Cluster, configPath string, backupDir string) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin cleanup for restore on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s cleanup_plugin_for_restore %s %s", plugin.ExecutablePath, configPath, backupDir)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath)
	}, true)
}

func (plugin *PluginConfig) CopyPluginConfigToAllHosts(c cluster.Cluster, configPath string) {
	remoteOutput := c.GenerateAndExecuteCommand("Copying plugin config to all hosts", func(contentID int) string {
		return fmt.Sprintf("rsync %s:%s /tmp/.", c.GetHostForContent(-1), configPath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, "Unable to copy plugin config", func(contentID int) string {
		return "Unable to copy plugin config"
	})
}

func (plugin *PluginConfig) BackupSegmentTOCs(c cluster.Cluster, fpInfo FilePathInfo) {
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
		return fmt.Sprintf("%s backup_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, tocFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return "See gpAdminLog for gpbackup_helper on segment host for details: Error occurred with plugin"
	})
}

func (plugin *PluginConfig) RestoreSegmentTOCs(c cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		return fmt.Sprintf("%s restore_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, tocFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}
