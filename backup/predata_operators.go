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

func PrintCreateOperatorStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operator Operator, operatorMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
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

CREATE OPERATOR %s.%s (
	PROCEDURE = %s,
	%s
);`, operator.Schema, operator.Name, operator.Procedure, strings.Join(optionalFields, ",\n\t"))

	section, entry := operator.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, operatorMetadata, operator, "")
}

/*
 * Operator families are not supported in GPDB 4.3, so this function
 * is not used in a 4.3 backup.
 */
func PrintCreateOperatorFamilyStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operatorFamilies []OperatorFamily, operatorFamilyMetadata MetadataMap) {
	for _, operatorFamily := range operatorFamilies {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nCREATE OPERATOR FAMILY %s;", operatorFamily.FQN())

		section, entry := operatorFamily.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, operatorFamilyMetadata[operatorFamily.GetUniqueID()], operatorFamily, "")
	}
}

func PrintCreateOperatorClassStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, operatorClass OperatorClass, operatorClassMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE OPERATOR CLASS %s.%s", operatorClass.Schema, operatorClass.Name)
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

	section, entry := operatorClass.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, operatorClassMetadata, operatorClass, "")
}
