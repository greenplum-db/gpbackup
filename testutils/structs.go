package testutils

/*
 * This file contains test structs and functions used in unit tests via dependency injection.
 */

import (
	"github.com/greenplum-db/gpbackup/utils"
)

type TestExecutor struct {
	LocalError      error
	LocalCommands   []string
	ClusterOutput   *utils.RemoteOutput
	ClusterCommands []map[int][]string
	ErrorOnExecNum  int // Throw the specified error after this many executions of Execute[...]Command(); 0 means always return error
	NumExecutions   int
}

func (executor *TestExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	executor.NumExecutions++
	executor.LocalCommands = append(executor.LocalCommands, commandStr)
	if executor.ErrorOnExecNum == 0 || executor.NumExecutions == executor.ErrorOnExecNum {
		return "", executor.LocalError
	}
	return "", nil
}

func (executor *TestExecutor) ExecuteClusterCommand(commandMap map[int][]string) *utils.RemoteOutput {
	executor.NumExecutions++
	executor.ClusterCommands = append(executor.ClusterCommands, commandMap)
	if executor.ErrorOnExecNum == 0 || executor.NumExecutions == executor.ErrorOnExecNum {
		return executor.ClusterOutput
	}
	return nil
}
