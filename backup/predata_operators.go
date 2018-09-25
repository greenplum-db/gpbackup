package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects that don't fall under any other predata categorization,
 * such as procedural languages and constraints, that needs to be restored
 * before data is restored.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateOperatorStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operators []Operator, operatorMetadata MetadataMap) {
	for _, operator := range operators {
		start := metadataFile.ByteCount
		// We do not use utils.MakeFQN here as the operator cannot be quoted
		operatorFQN := utils.MakeFQN(operator.Schema, operator.Name)
		optionalFields := make([]string, 0)
		leftArg := "NONE"
		rightArg := "NONE"
		if operator.LeftArgType != "-" {
			leftArg = operator.LeftArgType
			optionalFields = append(optionalFields, fmt.Sprintf("LEFTARG = %s", leftArg))
		}
		if operator.RightArgType != "-" {
			rightArg = operator.RightArgType
			optionalFields = append(optionalFields, fmt.Sprintf("RIGHTARG = %s", rightArg))
		}
		if operator.CommutatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("COMMUTATOR = OPERATOR(%s)", operator.CommutatorOp))
		}
		if operator.NegatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("NEGATOR = OPERATOR(%s)", operator.NegatorOp))
		}
		if operator.RestrictFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("RESTRICT = %s", operator.RestrictFunction))
		}
		if operator.JoinFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("JOIN = %s", operator.JoinFunction))
		}
		if operator.CanHash {
			optionalFields = append(optionalFields, "HASHES")
		}
		if operator.CanMerge {
			optionalFields = append(optionalFields, "MERGES")
		}
		metadataFile.MustPrintf(`

CREATE OPERATOR %s (
	PROCEDURE = %s,
	%s
);`, operatorFQN, operator.Procedure, strings.Join(optionalFields, ",\n\t"))
		operatorStr := fmt.Sprintf("%s (%s, %s)", operatorFQN, leftArg, rightArg)
		PrintObjectMetadata(metadataFile, operatorMetadata[operator.GetUniqueID()], operatorStr, "OPERATOR")
		toc.AddPredataEntry(operator.Schema, operator.Name, "OPERATOR", "", start, metadataFile)
	}
}

/*
 * Operator families are not supported in GPDB 4.3, so this function
 * is not used in a 4.3 backup.
 */
func PrintCreateOperatorFamilyStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operatorFamilies []OperatorFamily, operatorFamilyMetadata MetadataMap) {
	for _, operatorFamily := range operatorFamilies {
		start := metadataFile.ByteCount
		operatorFamilyFQN := utils.MakeFQN(operatorFamily.Schema, operatorFamily.Name)
		operatorFamilyStr := fmt.Sprintf("%s USING %s", operatorFamilyFQN, operatorFamily.IndexMethod)
		metadataFile.MustPrintf("\n\nCREATE OPERATOR FAMILY %s;", operatorFamilyStr)
		PrintObjectMetadata(metadataFile, operatorFamilyMetadata[operatorFamily.GetUniqueID()], operatorFamilyStr, "OPERATOR FAMILY")
		toc.AddPredataEntry(operatorFamily.Schema, operatorFamily.Name, "OPERATOR FAMILY", "", start, metadataFile)
	}
}

func PrintCreateOperatorClassStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operatorClasses []OperatorClass, operatorClassMetadata MetadataMap) {
	for _, operatorClass := range operatorClasses {
		start := metadataFile.ByteCount
		operatorClassFQN := utils.MakeFQN(operatorClass.Schema, operatorClass.Name)
		metadataFile.MustPrintf("\n\nCREATE OPERATOR CLASS %s", operatorClassFQN)
		forTypeStr := ""
		if operatorClass.Default {
			forTypeStr += "DEFAULT "
		}
		forTypeStr += fmt.Sprintf("FOR TYPE %s USING %s", operatorClass.Type, operatorClass.IndexMethod)
		if operatorClass.FamilyName != "" && operatorClass.FamilyName != operatorClass.Name {
			operatorFamilyFQN := utils.MakeFQN(operatorClass.FamilySchema, operatorClass.FamilyName)
			forTypeStr += fmt.Sprintf(" FAMILY %s", operatorFamilyFQN)
		}
		metadataFile.MustPrintf("\n\t%s", forTypeStr)
		opClassClauses := []string{}
		if len(operatorClass.Operators) != 0 {
			for _, operator := range operatorClass.Operators {
				opStr := fmt.Sprintf("OPERATOR %d %s", operator.StrategyNumber, operator.Operator)
				if operator.Recheck {
					opStr += " RECHECK"
				}
				if operator.OrderByFamily != "" {
					opStr += fmt.Sprintf(" FOR ORDER BY %s", operator.OrderByFamily)
				}
				opClassClauses = append(opClassClauses, opStr)
			}
		}
		if len(operatorClass.Functions) != 0 {
			for _, function := range operatorClass.Functions {
				var typeClause string
				if (function.LeftType != "") && (function.RightType != "") {
					typeClause = fmt.Sprintf("(%s, %s) ", function.LeftType, function.RightType)
				}
				opClassClauses = append(opClassClauses, fmt.Sprintf("FUNCTION %d %s%s", function.SupportNumber, typeClause, function.FunctionName))
			}
		}
		if operatorClass.StorageType != "-" || len(opClassClauses) == 0 {
			storageType := operatorClass.StorageType
			if operatorClass.StorageType == "-" {
				storageType = operatorClass.Type
			}
			opClassClauses = append(opClassClauses, fmt.Sprintf("STORAGE %s", storageType))
		}
		metadataFile.MustPrintf(" AS\n\t%s;", strings.Join(opClassClauses, ",\n\t"))

		operatorClassStr := fmt.Sprintf("%s USING %s", operatorClassFQN, operatorClass.IndexMethod)
		PrintObjectMetadata(metadataFile, operatorClassMetadata[operatorClass.GetUniqueID()], operatorClassStr, "OPERATOR CLASS")
		toc.AddPredataEntry(operatorClass.Schema, operatorClass.Name, "OPERATOR CLASS", "", start, metadataFile)
	}
}
