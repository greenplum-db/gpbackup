package utils

import (
	"fmt"
	"os"
	"os/exec"
	path "path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const RequiredPluginVersion = "0.3.0"
const SecretKeyFile = ".encrypt"

type PluginConfig struct {
	ExecutablePath      string            `yaml:"executablepath"`
	ConfigPath          string            `yaml:"-"`
	Options             map[string]string `yaml:"options"`
	backupPluginVersion string            `yaml:"-"`
}

type PluginScope string

// The COORDINATOR and MASTER scopes are identical in function, we just support
// both so that creators of existing plugins as of GPDB 6 need not (re)write
// them to support the GPDB 7 verbiage.  Plugin code should use COORDINATOR when
// carrying out internal functionality, but check for both COORDINATOR and MASTER
// when expecting external input.
const (
	COORDINATOR  PluginScope = "coordinator"
	MASTER       PluginScope = "master"
	SEGMENT_HOST PluginScope = "segment_host"
	SEGMENT      PluginScope = "segment"
)

func ReadPluginConfig(configFile string) (*PluginConfig, error) {
	gplog.Info("Reading Plugin Config %s", configFile)
	config := &PluginConfig{}
	contents, err := operating.System.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	err = yaml.UnmarshalStrict(contents, config)
	if err != nil {
		return nil, errors.New("plugin config file is formatted incorrectly")
	}
	if config.ExecutablePath == "" {
		return nil, errors.New("executablepath is required in config file")
	}
	if config.Options == nil {
		config.Options = make(map[string]string)
	}
	config.ExecutablePath = os.ExpandEnv(config.ExecutablePath)
	err = ValidateFullPath(config.ExecutablePath)
	if err != nil {
		return nil, err
	}
	configFilename := path.Base(configFile)
	config.ConfigPath = path.Join("/tmp", configFilename)
	return config, nil
}

func (plugin *PluginConfig) BackupFile(filenamePath string) error {
	command := fmt.Sprintf("%s backup_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, filenamePath)
	gplog.Debug("%s", command)
	output, err := exec.Command("bash", "-c", command).CombinedOutput()
	if err != nil {
		return fmt.Errorf("ERROR: Plugin failed to process %s. %s", filenamePath, string(output))
	}
	err = operating.System.Chmod(filenamePath, 0755)
	return err
}

func (plugin *PluginConfig) MustBackupFile(filenamePath string) {
	err := plugin.BackupFile(filenamePath)
	gplog.FatalOnError(err)
}

func (plugin *PluginConfig) MustRestoreFile(filenamePath string) {
	directory, _ := path.Split(filenamePath)
	err := operating.System.MkdirAll(directory, 0755)
	gplog.FatalOnError(err)
	command := fmt.Sprintf("%s restore_file %s %s", plugin.ExecutablePath, plugin.ConfigPath, filenamePath)
	gplog.Debug("%s", command)
	output, err := exec.Command("bash", "-c", command).CombinedOutput()
	gplog.FatalOnError(err, string(output))
}

func (plugin *PluginConfig) CheckPluginExistsOnAllHosts(c *cluster.Cluster) string {
	plugin.checkPluginAPIVersion(c)

	return plugin.getPluginNativeVersion(c)
}

func (plugin *PluginConfig) checkPluginAPIVersion(c *cluster.Cluster) {
	command := fmt.Sprintf("source %s/greenplum_path.sh && %s plugin_api_version",
		operating.System.Getenv("GPHOME"), plugin.ExecutablePath)
	remoteOutput := c.GenerateAndExecuteCommand(
		"Checking plugin api version on all hosts",
		cluster.ON_HOSTS&cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return command
		})
	gplog.Debug("%s", command)
	c.CheckClusterError(
		remoteOutput,
		fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath),
		func(contentID int) string {
			return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
		})
	requiredVersion, err := semver.Make(RequiredPluginVersion)
	if err != nil {
		gplog.Fatal(fmt.Errorf("cannot parse hardcoded internal string of required version: %s",
			err.Error()), RequiredPluginVersion)
	}
	numIncorrect := 0
	var pluginVersion string
	var version semver.Version
	index := 0
	for contentID, cmd := range remoteOutput.Commands {
		// check consistency of plugin version across all segments
		tempPluginVersion := strings.TrimSpace(cmd.Stdout)
		if pluginVersion != "" && tempPluginVersion != "" {
			if pluginVersion != tempPluginVersion {
				gplog.Verbose("Plugin %s on content ID %v with API version %s is not consistent "+
					"with version on another segment", plugin.ExecutablePath, contentID, version)
				cluster.LogFatalClusterError("Plugin API version is inconsistent "+
					"across segments; please reinstall plugin across segments",
					cluster.ON_HOSTS&cluster.INCLUDE_COORDINATOR, numIncorrect)
			}
		}

		pluginVersion = tempPluginVersion
		version, err = semver.Make(pluginVersion)
		if err != nil {
			gplog.Fatal(fmt.Errorf("ERROR: Unable to parse plugin API version: %s", err.Error()), "")
		}
		if !version.GE(requiredVersion) {
			gplog.Verbose("Plugin %s API version %s is not compatible with supported API "+
				"version %s", plugin.ExecutablePath, version, requiredVersion)
			numIncorrect++
		}
		index++
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("Plugin API version incorrect",
			cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, numIncorrect)
	}
}

func (plugin *PluginConfig) getPluginNativeVersion(c *cluster.Cluster) string {
	command := fmt.Sprintf("source %s/greenplum_path.sh && %s --version",
		operating.System.Getenv("GPHOME"), plugin.ExecutablePath)
	remoteOutput := c.GenerateAndExecuteCommand(
		"Checking plugin version on all hosts",
		cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return command
		})
	gplog.Debug("%s", command)
	c.CheckClusterError(
		remoteOutput,
		fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath),
		func(contentID int) string {
			return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
		})
	numIncorrect := 0
	var pluginVersion string
	index := 0
	badPluginVersion := ""
	var parts []string
	for contentID, cmd := range remoteOutput.Commands {
		tempPluginVersion := strings.TrimSpace(cmd.Stdout)
		// check consistency of plugin version across all segments
		if pluginVersion != "" && tempPluginVersion != "" {
			if pluginVersion != tempPluginVersion {
				gplog.Verbose("Plugin %s on content ID %v with --version %s is not consistent "+
					"with version on another segment", plugin.ExecutablePath, contentID, pluginVersion)
				cluster.LogFatalClusterError("Plugin --version is inconsistent "+
					"across segments; please reinstall plugin across segments",
					cluster.ON_HOSTS&cluster.INCLUDE_COORDINATOR, numIncorrect)
			}
		}

		parts = strings.Split(tempPluginVersion, " ")
		if len(parts) < 3 {
			numIncorrect++
			badPluginVersion = tempPluginVersion
		} else {
			pluginVersion = tempPluginVersion
		}
		index++
	}
	if numIncorrect > 0 || pluginVersion == "" {
		cluster.LogFatalClusterError(fmt.Sprintf("Plugin --version response '%s' incorrect", badPluginVersion),
			cluster.ON_HOSTS&cluster.INCLUDE_COORDINATOR, numIncorrect)
	}
	return parts[2]
}

/*-----------------------------Hooks------------------------------------------*/

func (plugin *PluginConfig) SetupPluginForBackup(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	const command = "setup_plugin_for_backup"
	const verboseCommandMsg = "Running plugin setup for backup on %s"
	plugin.executeHook(c, verboseCommandMsg, command, fpInfo, false)
}

func (plugin *PluginConfig) SetupPluginForRestore(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	const command = "setup_plugin_for_restore"
	const verboseCommandMsg = "Running plugin setup for restore on %s"
	plugin.executeHook(c, verboseCommandMsg, command, fpInfo, false)
}

func (plugin *PluginConfig) CleanupPluginForBackup(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	const command = "cleanup_plugin_for_backup"
	const verboseCommandMsg = "Running plugin cleanup for backup on %s"
	plugin.executeHook(c, verboseCommandMsg, command, fpInfo, true)
}

func (plugin *PluginConfig) CleanupPluginForRestore(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	const command = "cleanup_plugin_for_restore"
	const verboseCommandMsg = "Running plugin cleanup for restore on %s"
	plugin.executeHook(c, verboseCommandMsg, command, fpInfo, true)
}

func (plugin *PluginConfig) executeHook(c *cluster.Cluster, verboseCommandMsg string,
	command string, fpInfo filepath.FilePathInfo, noFatal bool) {

	// Execute command once on coordinator
	scope := MASTER
	_, _ = plugin.buildHookErrorMsgAndFunc(command, scope)
	coordinatorContentID := -1
	coordinatorOutput, coordinatorErr := c.ExecuteLocalCommand(
		plugin.buildHookString(command, fpInfo, scope, coordinatorContentID))
	if coordinatorErr != nil {
		if noFatal {
			gplog.Error(coordinatorOutput)
			return
		}
		gplog.Fatal(coordinatorErr, coordinatorOutput)
	}

	// Execute command once on each segment host
	scope = SEGMENT_HOST
	hookFunc := plugin.buildHookFunc(command, fpInfo, scope)
	verboseErrorMsg, errorMsgFunc := plugin.buildHookErrorMsgAndFunc(command, scope)
	verboseCommandHostCoordinatorMsg := fmt.Sprintf(verboseCommandMsg, "segment hosts")
	remoteOutput := c.GenerateAndExecuteCommand(verboseCommandHostCoordinatorMsg, cluster.ON_HOSTS, hookFunc)
	gplog.Debug("Execute Hook: %s", command)
	c.CheckClusterError(remoteOutput, verboseErrorMsg, errorMsgFunc, noFatal)

	// Execute command once for each segment
	scope = SEGMENT
	hookFunc = plugin.buildHookFunc(command, fpInfo, scope)
	verboseErrorMsg, errorMsgFunc = plugin.buildHookErrorMsgAndFunc(command, scope)
	verboseCommandSegMsg := fmt.Sprintf(verboseCommandMsg, "segments")
	remoteOutput = c.GenerateAndExecuteCommand(verboseCommandSegMsg, cluster.ON_SEGMENTS, hookFunc)
	c.CheckClusterError(remoteOutput, verboseErrorMsg, errorMsgFunc, noFatal)
}

func (plugin *PluginConfig) buildHookFunc(command string,
	fpInfo filepath.FilePathInfo, scope PluginScope) func(int) string {
	return func(contentID int) string {
		return plugin.buildHookString(command, fpInfo, scope, contentID)
	}
}

func (plugin *PluginConfig) buildHookString(command string,
	fpInfo filepath.FilePathInfo, scope PluginScope, contentID int) string {
	contentIDStr := ""
	if scope == COORDINATOR || scope == MASTER || scope == SEGMENT {
		contentIDStr = fmt.Sprintf(`\"%d\"`, contentID)
	}

	backupDir := fpInfo.GetDirForContent(contentID)
	return fmt.Sprintf("source %s/greenplum_path.sh && %s %s %s %s %s %s",
		operating.System.Getenv("GPHOME"), plugin.ExecutablePath, command,
		plugin.ConfigPath, backupDir, scope, contentIDStr)
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

func (plugin *PluginConfig) CopyPluginConfigToAllHosts(c *cluster.Cluster) {
	// create a unique config file per segment in order to convey the PGPORT for the segment
	// to the plugin.  At some point in the future, the plugin MAY be able to get PGPORT as
	// an environmental var, at which time the code to write *specific* config files per segment
	// can be removed
	var command string
	rsync_exists := CommandExists("rsync")
	if !rsync_exists {
		gplog.Fatal(errors.New("Failed to find rsync on PATH. Please ensure rsync is installed."), "")
	}
	remoteOutput := c.GenerateAndExecuteCommand(
		"Copying plugin config to all hosts",
		cluster.ON_LOCAL|cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
		func(contentIDForSegmentOnHost int) string {
			hostConfigFile := plugin.createHostPluginConfig(contentIDForSegmentOnHost, c)
			command = fmt.Sprintf("rsync -e ssh %[1]s %s:%s; rm %[1]s", hostConfigFile,
				c.GetHostForContent(contentIDForSegmentOnHost), plugin.ConfigPath)
			return command
		})
	gplog.Debug("%s", command)
	errMsg := "Unable to copy plugin config"
	c.CheckClusterError(
		remoteOutput,
		errMsg,
		func(contentID int) string {
			return errMsg
		},
	)
}

func (plugin *PluginConfig) DeletePluginConfigWhenEncrypting(c *cluster.Cluster) {
	if !plugin.UsesEncryption() {
		return
	}

	verboseMsg := "Removing plugin config from all hosts"
	scope := cluster.ON_HOSTS | cluster.INCLUDE_COORDINATOR
	command := fmt.Sprintf("rm -f %s", plugin.ConfigPath)
	f := func(contentIDForSegmentOnHost int) string {
		return command
	}
	remoteOutput := c.GenerateAndExecuteCommand(verboseMsg, scope, f)
	gplog.Debug("%s", command)
	errMsg := "Unable to remove plugin config"
	c.CheckClusterError(
		remoteOutput,
		errMsg,
		func(contentID int) string {
			return errMsg
		},
		true,
	)
}

// Creates a valid segment-specific plugin configuration file with unique name
func (plugin *PluginConfig) createHostPluginConfig(contentIDForSegmentOnHost int,
	c *cluster.Cluster) string {
	// copy "general" config file to temp, and add segment-specific PGPORT value

	segmentSpecificConfigFile := plugin.ConfigPath + "_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.Itoa(contentIDForSegmentOnHost)
	file := iohelper.MustOpenFileForWriting(segmentSpecificConfigFile)

	// add current pgport as attribute
	plugin.Options["pgport"] = strconv.Itoa(c.GetPortForContent(contentIDForSegmentOnHost))
	plugin.Options["backup_plugin_version"] = plugin.BackupPluginVersion()
	if plugin.UsesEncryption() {
		pluginName, err := plugin.GetPluginName(c)
		if err != nil {
			_, _ = fmt.Fprintf(operating.System.Stdout, err.Error())
			gplog.Fatal(nil, err.Error())
		}

		secret, err := GetSecretKey(pluginName, c.GetDirForContent(-1))
		if err != nil {
			_, _ = fmt.Fprintf(operating.System.Stdout, err.Error())
			gplog.Fatal(nil, err.Error())
		}
		plugin.Options[pluginName] = secret
	}
	out, err := yaml.Marshal(plugin)
	gplog.FatalOnError(err)
	bytes, err := file.Write(out)
	gplog.FatalOnError(err)
	err = file.Close()
	gplog.FatalOnError(err)
	gplog.Debug("Wrote %d bytes to plugin config %s", bytes, segmentSpecificConfigFile)
	return segmentSpecificConfigFile
}

func GetSecretKey(pluginName string, mdd string) (string, error) {
	secretFilePath := path.Join(mdd, SecretKeyFile)
	contents, err := operating.System.ReadFile(secretFilePath)

	errMsg := fmt.Sprintf("Cannot find encryption key for plugin %s. "+
		"Please re-encrypt password(s) so that key becomes available.", pluginName)
	if err != nil {
		return "", errors.New(errMsg)
	}
	keys := make(map[string]string)
	_ = yaml.Unmarshal(contents, keys) // if error happens, we catch it because no keys exist
	key, exists := keys[pluginName]
	if !exists {
		return "", errors.New(errMsg)
	}
	return key, nil

}

func (plugin *PluginConfig) BackupSegmentTOCs(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	var command string
	remoteOutput := c.GenerateAndExecuteCommand("Waiting for remaining data to be uploaded to plugin destination",
		cluster.ON_SEGMENTS,
		func(contentID int) string {
			tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
			errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
			command = fmt.Sprintf(`while [[ ! -f "%s" && ! -f "%s" ]]; do sleep 1; done; ls "%s"`, tocFile, errorFile, tocFile)
			return command
		})
	gplog.Debug("%s", command)
	c.CheckClusterError(remoteOutput, "Error occurred in gpbackup_helper", func(contentID int) string {
		return "See gpAdminLog for gpbackup_helper on segment host for details: Error occurred with plugin"
	})

	remoteOutput = c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", cluster.ON_SEGMENTS,
		func(contentID int) string {
			tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
			return fmt.Sprintf("source %s/greenplum_path.sh && %s backup_file %s %s && "+
				"chmod 0755 %s", operating.System.Getenv("GPHOME"), plugin.ExecutablePath, plugin.ConfigPath, tocFile, tocFile)
		})
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return "See gpAdminLog for gpbackup_helper on segment host for details: Error occurred with plugin"
	})
}

func (plugin *PluginConfig) RestoreSegmentTOCs(c *cluster.Cluster, fpInfo filepath.FilePathInfo, isResizeRestore bool, origSize int, destSize int) {
	var command string
	batches := 1
	if isResizeRestore {
		batches = origSize / destSize
		if origSize%destSize != 0 {
			batches += 1
		}
	}
	for b := 0; b < batches; b++ {
		remoteOutput := c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", cluster.ON_SEGMENTS, func(contentID int) string {
			origContent := contentID + b*destSize
			if origContent >= origSize { // Don't try to restore files for contents that aren't part of the backup set
				return ""
			}
			tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
			// Restore the filename with the origin content to the directory with the destination content
			tocFile = strings.ReplaceAll(tocFile, fmt.Sprintf("gpbackup_%d", contentID), fmt.Sprintf("gpbackup_%d", origContent))
			command = fmt.Sprintf("mkdir -p %s && source %s/greenplum_path.sh && %s restore_file %s %s",
				fpInfo.GetDirForContent(contentID), operating.System.Getenv("GPHOME"),
				plugin.ExecutablePath, plugin.ConfigPath, tocFile)
			return command
		})
		gplog.Debug("%s", command)
		c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
			return fmt.Sprintf("Unable to process segment TOC files using plugin")
		})
	}
}

func (plugin *PluginConfig) UsesEncryption() bool {
	return plugin.Options["password_encryption"] == "on" ||
		(plugin.Options["replication"] == "on" && plugin.Options["remote_password_encryption"] == "on")
}

func (plugin *PluginConfig) GetPluginName(c *cluster.Cluster) (pluginName string, err error) {
	pluginCall := fmt.Sprintf("%s --version", plugin.ExecutablePath)
	output, err := c.ExecuteLocalCommand(pluginCall)
	if err != nil {
		return "", fmt.Errorf("ERROR: Failed to get plugin name. Failed with error: %s", err.Error())
	}

	// expects the output to be in "[plugin_name] version [git_version]"
	s := strings.Split(output, " ")
	if len(s) != 3 {
		return "", fmt.Errorf("Unexpected plugin version format: "+
			"\"%s\"\nExpected: \"[plugin_name] version [git_version]\"", strings.Join(s, " "))
	}

	return s[0], nil
}

func (plugin *PluginConfig) BackupPluginVersion() string {
	return plugin.backupPluginVersion
}

func (plugin *PluginConfig) SetBackupPluginVersion(timestamp string, historicalPluginVersion string) {
	if historicalPluginVersion == "" {
		gplog.Warn("cannot recover plugin version from history using timestamp %s, "+
			"so using current plugin version. This is fine unless there is a backwards "+
			"compatibility consideration within the plugin", timestamp)
		plugin.backupPluginVersion = ""
	} else {
		plugin.backupPluginVersion = historicalPluginVersion
	}
}

func (plugin *PluginConfig) CanRestoreSubset() bool {
	return (plugin.Options["restore_subset"] == "on") ||
		(strings.HasSuffix(plugin.ExecutablePath, "ddboost_plugin") &&
			plugin.Options["restore_subset"] != "off")
}
