package backup

/*
 * This file contains structs and functions related to backing up function
 * metadata, and metadata closely related to functions such as aggregates
 * and casts, that needs to be restored before data is restored.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateFunctionStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, funcDef Function, funcMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	funcFQN := utils.MakeFQN(funcDef.Schema, funcDef.Name)
	predataFile.MustPrintf("\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
	predataFile.MustPrintf("%s AS", funcDef.ResultType)
	PrintFunctionBodyOrPath(predataFile, funcDef)
	predataFile.MustPrintf("LANGUAGE %s", funcDef.Language)
	PrintFunctionModifiers(predataFile, funcDef)
	predataFile.MustPrintln(";")

	nameStr := fmt.Sprintf("%s(%s)", funcFQN, funcDef.IdentArgs)
	nameWithArgs := fmt.Sprintf("%s(%s)", funcDef.Name, funcDef.IdentArgs)
	PrintObjectMetadata(predataFile, funcMetadata, nameStr, "FUNCTION")
	toc.AddMetadataEntry(funcDef.Schema, nameWithArgs, "FUNCTION", start, predataFile)
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(predataFile *utils.FileWithByteCount, funcDef Function) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		predataFile.MustPrintf("\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		predataFile.MustPrintf("\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(predataFile *utils.FileWithByteCount, funcDef Function) {
	switch funcDef.DataAccess {
	case "c":
		predataFile.MustPrintf(" CONTAINS SQL")
	case "m":
		predataFile.MustPrintf(" MODIFIES SQL DATA")
	case "n":
		predataFile.MustPrintf(" NO SQL")
	case "r":
		predataFile.MustPrintf(" READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		predataFile.MustPrintf(" IMMUTABLE")
	case "s":
		predataFile.MustPrintf(" STABLE")
	case "v": // Default case, don't print anything else
	}
	if funcDef.IsStrict {
		predataFile.MustPrintf(" STRICT")
	}
	if funcDef.IsSecurityDefiner {
		predataFile.MustPrintf(" SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1) || funcDef.Cost == 0) {
		predataFile.MustPrintf("\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		predataFile.MustPrintf("\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		predataFile.MustPrintf("\n%s", funcDef.Config)
	}
}

func PrintCreateAggregateStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, aggDefs []Aggregate, funcInfoMap map[uint32]FunctionInfo, aggMetadata MetadataMap) {
	for _, aggDef := range aggDefs {
		start := predataFile.ByteCount
		aggFQN := utils.MakeFQN(aggDef.Schema, aggDef.Name)
		orderedStr := ""
		if aggDef.IsOrdered {
			orderedStr = "ORDERED "
		}
		argumentsStr := "*"
		if aggDef.Arguments != "" {
			argumentsStr = aggDef.Arguments
		}
		predataFile.MustPrintf("\n\nCREATE %sAGGREGATE %s(%s) (\n", orderedStr, aggFQN, argumentsStr)

		predataFile.MustPrintf("\tSFUNC = %s,\n", funcInfoMap[aggDef.TransitionFunction].QualifiedName)
		predataFile.MustPrintf("\tSTYPE = %s", aggDef.TransitionDataType)

		if aggDef.PreliminaryFunction != 0 {
			predataFile.MustPrintf(",\n\tPREFUNC = %s", funcInfoMap[aggDef.PreliminaryFunction].QualifiedName)
		}
		if aggDef.FinalFunction != 0 {
			predataFile.MustPrintf(",\n\tFINALFUNC = %s", funcInfoMap[aggDef.FinalFunction].QualifiedName)
		}
		if aggDef.FinalFuncExtra {
			predataFile.MustPrintf(",\n\tFINALFUNC_EXTRA")
		}
		if !aggDef.InitValIsNull {
			predataFile.MustPrintf(",\n\tINITCOND = '%s'", aggDef.InitialValue)
		}
		if aggDef.SortOperator != 0 {
			predataFile.MustPrintf(",\n\tSORTOP = %s", funcInfoMap[aggDef.SortOperator].QualifiedName)
		}
		if aggDef.Hypothetical {
			predataFile.MustPrintf(",\n\tHYPOTHETICAL")
		}
		predataFile.MustPrintln("\n);")

		identArgumentsStr := "*"
		if aggDef.IdentArgs != "" {
			identArgumentsStr = aggDef.IdentArgs
		}
		aggFQN = fmt.Sprintf("%s(%s)", aggFQN, identArgumentsStr)
		aggWithArgs := fmt.Sprintf("%s(%s)", aggDef.Name, identArgumentsStr)
		PrintObjectMetadata(predataFile, aggMetadata[aggDef.Oid], aggFQN, "AGGREGATE")
		toc.AddMetadataEntry(aggDef.Schema, aggWithArgs, "AGGREGATE", start, predataFile)
	}
}

func PrintCreateCastStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, castDefs []Cast, castMetadata MetadataMap) {
	for _, castDef := range castDefs {
		start := predataFile.ByteCount
		castStr := fmt.Sprintf("(%s AS %s)", castDef.SourceTypeFQN, castDef.TargetTypeFQN)
		predataFile.MustPrintf("\n\nCREATE CAST %s\n", castStr)
		if castDef.FunctionSchema != "" {
			funcFQN := utils.MakeFQN(castDef.FunctionSchema, castDef.FunctionName)
			predataFile.MustPrintf("\tWITH FUNCTION %s(%s)", funcFQN, castDef.FunctionArgs)
		} else {
			predataFile.MustPrintf("\tWITHOUT FUNCTION")
		}
		switch castDef.CastContext {
		case "a":
			predataFile.MustPrintf("\nAS ASSIGNMENT")
		case "i":
			predataFile.MustPrintf("\nAS IMPLICIT")
		case "e": // Default case, don't print anything else
		}
		predataFile.MustPrintf(";")
		PrintObjectMetadata(predataFile, castMetadata[castDef.Oid], castStr, "CAST")
		toc.AddMetadataEntry("pg_catalog", castStr, "CAST", start, predataFile)
	}
}

/*
 * This function separates out functions related to procedural languages from
 * any other functions, so that language-related functions can be backed up before
 * the languages themselves and we can avoid sorting languages and functions
 * together to resolve dependencies.
 */
func ExtractLanguageFunctions(funcDefs []Function, procLangs []ProceduralLanguage) ([]Function, []Function) {
	isLangFuncMap := make(map[uint32]bool, 0)
	for _, procLang := range procLangs {
		for _, funcDef := range funcDefs {
			isLangFuncMap[funcDef.Oid] = (funcDef.Oid == procLang.Handler ||
				funcDef.Oid == procLang.Inline ||
				funcDef.Oid == procLang.Validator)
		}
	}
	langFuncs := make([]Function, 0)
	otherFuncs := make([]Function, 0)
	for _, funcDef := range funcDefs {
		if isLangFuncMap[funcDef.Oid] {
			langFuncs = append(langFuncs, funcDef)
		} else {
			otherFuncs = append(otherFuncs, funcDef)
		}
	}
	return langFuncs, otherFuncs
}

func PrintCreateLanguageStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, procLangs []ProceduralLanguage,
	funcInfoMap map[uint32]FunctionInfo, procLangMetadata MetadataMap) {
	for _, procLang := range procLangs {
		start := predataFile.ByteCount
		predataFile.MustPrintf("\n\nCREATE ")
		if procLang.PlTrusted {
			predataFile.MustPrintf("TRUSTED ")
		}
		predataFile.MustPrintf("PROCEDURAL LANGUAGE %s;", procLang.Name)
		/*
		 * If the handler, validator, and inline functions are in pg_pltemplate, we can
		 * back up a CREATE LANGUAGE command without specifying them individually.
		 *
		 * The schema of the handler function should match the schema of the language itself, but
		 * the inline and validator functions can be in a different schema and must be schema-qualified.
		 */

		if procLang.Handler != 0 {
			handlerInfo := funcInfoMap[procLang.Handler]
			predataFile.MustPrintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", handlerInfo.QualifiedName, handlerInfo.Arguments, procLang.Owner)
		}
		if procLang.Inline != 0 {
			inlineInfo := funcInfoMap[procLang.Inline]
			predataFile.MustPrintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", inlineInfo.QualifiedName, inlineInfo.Arguments, procLang.Owner)
		}
		if procLang.Validator != 0 {
			validatorInfo := funcInfoMap[procLang.Validator]
			predataFile.MustPrintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", validatorInfo.QualifiedName, validatorInfo.Arguments, procLang.Owner)
		}
		PrintObjectMetadata(predataFile, procLangMetadata[procLang.Oid], procLang.Name, "LANGUAGE")
		predataFile.MustPrintln()
		toc.AddMetadataEntry("", procLang.Name, "PROCEDURAL LANGUAGE", start, predataFile)
	}
}

func PrintCreateConversionStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, conversions []Conversion, conversionMetadata MetadataMap) {
	for _, conversion := range conversions {
		start := predataFile.ByteCount
		convFQN := utils.MakeFQN(conversion.Schema, conversion.Name)
		defaultStr := ""
		if conversion.IsDefault {
			defaultStr = " DEFAULT"
		}
		predataFile.MustPrintf("\n\nCREATE%s CONVERSION %s FOR '%s' TO '%s' FROM %s;",
			defaultStr, convFQN, conversion.ForEncoding, conversion.ToEncoding, conversion.ConversionFunction)
		PrintObjectMetadata(predataFile, conversionMetadata[conversion.Oid], convFQN, "CONVERSION")
		predataFile.MustPrintln()
		toc.AddMetadataEntry(conversion.Schema, conversion.Name, "CONVERSION", start, predataFile)
	}
}
