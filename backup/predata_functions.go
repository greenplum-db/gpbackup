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

func PrintCreateFunctionStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, funcDef Function, funcMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	funcFQN := utils.MakeFQN(funcDef.Schema, funcDef.Name)
	metadataFile.MustPrintf("\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
	metadataFile.MustPrintf("%s AS", funcDef.ResultType)
	PrintFunctionBodyOrPath(metadataFile, funcDef)
	metadataFile.MustPrintf("LANGUAGE %s", funcDef.Language)
	PrintFunctionModifiers(metadataFile, funcDef)
	metadataFile.MustPrintln(";")

	section, entry := funcDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, funcMetadata, funcDef, "")
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(metadataFile *utils.FileWithByteCount, funcDef Function) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		metadataFile.MustPrintf("\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		metadataFile.MustPrintf("\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(metadataFile *utils.FileWithByteCount, funcDef Function) {
	switch funcDef.DataAccess {
	case "c":
		metadataFile.MustPrintf(" CONTAINS SQL")
	case "m":
		metadataFile.MustPrintf(" MODIFIES SQL DATA")
	case "n":
		metadataFile.MustPrintf(" NO SQL")
	case "r":
		metadataFile.MustPrintf(" READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		metadataFile.MustPrintf(" IMMUTABLE")
	case "s":
		metadataFile.MustPrintf(" STABLE")
	case "v": // Default case, don't print anything else
	}
	switch funcDef.ExecLocation {
	case "m":
		metadataFile.MustPrintf(" EXECUTE ON MASTER")
	case "s":
		metadataFile.MustPrintf(" EXECUTE ON ALL SEGMENTS")
	case "a": // Default case, don't print anything else
	}
	if funcDef.IsWindow {
		metadataFile.MustPrintf(" WINDOW")
	}
	if funcDef.IsStrict {
		metadataFile.MustPrintf(" STRICT")
	}
	if funcDef.IsLeakProof {
		metadataFile.MustPrintf(" LEAKPROOF")
	}
	if funcDef.IsSecurityDefiner {
		metadataFile.MustPrintf(" SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1) || funcDef.Cost == 0) {
		metadataFile.MustPrintf("\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		metadataFile.MustPrintf("\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		metadataFile.MustPrintf("\n%s", funcDef.Config)
	}
}

func PrintCreateAggregateStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, aggDef Aggregate, funcInfoMap map[uint32]FunctionInfo, aggMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	orderedStr := ""
	if aggDef.IsOrdered {
		orderedStr = "ORDERED "
	}
	argumentsStr := "*"
	if aggDef.Arguments != "" {
		argumentsStr = aggDef.Arguments
	}
	metadataFile.MustPrintf("\n\nCREATE %sAGGREGATE %s.%s(%s) (\n", orderedStr, aggDef.Schema, aggDef.Name, argumentsStr)

	metadataFile.MustPrintf("\tSFUNC = %s,\n", funcInfoMap[aggDef.TransitionFunction].QualifiedName)
	metadataFile.MustPrintf("\tSTYPE = %s", aggDef.TransitionDataType)

	if aggDef.TransitionDataSize != 0 {
		metadataFile.MustPrintf(",\n\tSSPACE = %d", aggDef.TransitionDataSize)
	}
	if aggDef.PreliminaryFunction != 0 {
		metadataFile.MustPrintf(",\n\tPREFUNC = %s", funcInfoMap[aggDef.PreliminaryFunction].QualifiedName)
	}
	if aggDef.CombineFunction != 0 {
		metadataFile.MustPrintf(",\n\tCOMBINEFUNC = %s", funcInfoMap[aggDef.CombineFunction].QualifiedName)
	}
	if aggDef.SerialFunction != 0 {
		metadataFile.MustPrintf(",\n\tSERIALFUNC = %s", funcInfoMap[aggDef.SerialFunction].QualifiedName)
	}
	if aggDef.DeserialFunction != 0 {
		metadataFile.MustPrintf(",\n\tDESERIALFUNC = %s", funcInfoMap[aggDef.DeserialFunction].QualifiedName)
	}
	if aggDef.FinalFunction != 0 {
		metadataFile.MustPrintf(",\n\tFINALFUNC = %s", funcInfoMap[aggDef.FinalFunction].QualifiedName)
	}
	if aggDef.FinalFuncExtra {
		metadataFile.MustPrintf(",\n\tFINALFUNC_EXTRA")
	}
	if !aggDef.InitValIsNull {
		metadataFile.MustPrintf(",\n\tINITCOND = '%s'", aggDef.InitialValue)
	}
	if aggDef.SortOperator != "" {
		metadataFile.MustPrintf(",\n\tSORTOP = %s.\"%s\"", aggDef.SortOperatorSchema, aggDef.SortOperator)
	}
	if aggDef.Hypothetical {
		metadataFile.MustPrintf(",\n\tHYPOTHETICAL")
	}
	if aggDef.MTransitionFunction != 0 {
		metadataFile.MustPrintf(",\n\tMSFUNC = %s", funcInfoMap[aggDef.MTransitionFunction].QualifiedName)
	}
	if aggDef.MInverseTransitionFunction != 0 {
		metadataFile.MustPrintf(",\n\tMINVFUNC = %s", funcInfoMap[aggDef.MInverseTransitionFunction].QualifiedName)
	}
	if aggDef.MTransitionDataType != "" {
		metadataFile.MustPrintf(",\n\tMSTYPE = %s", aggDef.MTransitionDataType)
	}
	if aggDef.MTransitionDataSize != 0 {
		metadataFile.MustPrintf(",\n\tMSSPACE = %d", aggDef.MTransitionDataSize)
	}
	if aggDef.MFinalFunction != 0 {
		metadataFile.MustPrintf(",\n\tMFINALFUNC = %s", funcInfoMap[aggDef.MFinalFunction].QualifiedName)
	}
	if aggDef.MFinalFuncExtra {
		metadataFile.MustPrintf(",\n\tMFINALFUNC_EXTRA")
	}
	if !aggDef.MInitValIsNull {
		metadataFile.MustPrintf(",\n\tMINITCOND = '%s'", aggDef.MInitialValue)
	}
	metadataFile.MustPrintln("\n);")

	section, entry := aggDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, aggMetadata, aggDef, "")
}

func PrintCreateCastStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, castDef Cast, castMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE CAST %s\n", castDef.FQN())
	switch castDef.CastMethod {
	case "i":
		metadataFile.MustPrintf("\tWITH INOUT")
	case "b":
		metadataFile.MustPrintf("\tWITHOUT FUNCTION")
	case "f":
		funcFQN := utils.MakeFQN(castDef.FunctionSchema, castDef.FunctionName)
		metadataFile.MustPrintf("\tWITH FUNCTION %s(%s)", funcFQN, castDef.FunctionArgs)
	}
	switch castDef.CastContext {
	case "a":
		metadataFile.MustPrintf("\nAS ASSIGNMENT")
	case "i":
		metadataFile.MustPrintf("\nAS IMPLICIT")
	case "e": // Default case, don't print anything else
	}
	metadataFile.MustPrintf(";")

	section, entry := castDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, castMetadata, castDef, "")
}

func PrintCreateExtensionStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, extensionDefs []Extension, extensionMetadata MetadataMap) {
	for _, extensionDef := range extensionDefs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nSET search_path=%s,pg_catalog;\nCREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\nSET search_path=pg_catalog;", extensionDef.Schema, extensionDef.Name, extensionDef.Schema)

		section, entry := extensionDef.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, extensionMetadata[extensionDef.GetUniqueID()], extensionDef, "")
	}
}

/*
 * This function separates out functions related to procedural languages from
 * any other functions, so that language-related functions can be backed up before
 * the languages themselves and we can avoid sorting languages and functions
 * together to resolve dependencies.
 */
func ExtractLanguageFunctions(funcDefs []Function, procLangs []ProceduralLanguage) ([]Function, []Function) {
	isLangFuncMap := make(map[uint32]bool)
	for _, procLang := range procLangs {
		for _, funcDef := range funcDefs {
			isLangFuncMap[funcDef.Oid] = isLangFuncMap[funcDef.Oid] ||
				funcDef.Oid == procLang.Handler ||
				funcDef.Oid == procLang.Inline ||
				funcDef.Oid == procLang.Validator
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

func PrintCreateLanguageStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, procLangs []ProceduralLanguage,
	funcInfoMap map[uint32]FunctionInfo, procLangMetadata MetadataMap) {
	for _, procLang := range procLangs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nCREATE ")
		if procLang.PlTrusted {
			metadataFile.MustPrintf("TRUSTED ")
		}
		metadataFile.MustPrintf("PROCEDURAL LANGUAGE %s", procLang.Name)
		paramsStr := ""
		alterStr := ""
		/*
		 * If the handler, validator, and inline functions are in pg_pltemplate, we can
		 * back up a CREATE LANGUAGE command without specifying them individually.
		 *
		 * The schema of the handler function should match the schema of the language itself, but
		 * the inline and validator functions can be in a different schema and must be schema-qualified.
		 */

		if procLang.Handler != 0 {
			handlerInfo := funcInfoMap[procLang.Handler]
			paramsStr += fmt.Sprintf(" HANDLER %s", handlerInfo.QualifiedName)
			alterStr += fmt.Sprintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", handlerInfo.QualifiedName, handlerInfo.Arguments, procLang.Owner)
		}
		if procLang.Inline != 0 {
			inlineInfo := funcInfoMap[procLang.Inline]
			paramsStr += fmt.Sprintf(" INLINE %s", inlineInfo.QualifiedName)
			alterStr += fmt.Sprintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", inlineInfo.QualifiedName, inlineInfo.Arguments, procLang.Owner)
		}
		if procLang.Validator != 0 {
			validatorInfo := funcInfoMap[procLang.Validator]
			paramsStr += fmt.Sprintf(" VALIDATOR %s", validatorInfo.QualifiedName)
			alterStr += fmt.Sprintf("\nALTER FUNCTION %s(%s) OWNER TO %s;", validatorInfo.QualifiedName, validatorInfo.Arguments, procLang.Owner)
		}
		metadataFile.MustPrintf("%s;", paramsStr)

		section, entry := procLang.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		start = metadataFile.ByteCount
		metadataFile.MustPrint(alterStr)
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		PrintObjectMetadata(metadataFile, toc, procLangMetadata[procLang.GetUniqueID()], procLang, "")
	}
}

func PrintCreateConversionStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, conversions []Conversion, conversionMetadata MetadataMap) {
	for _, conversion := range conversions {
		start := metadataFile.ByteCount
		convFQN := utils.MakeFQN(conversion.Schema, conversion.Name)
		defaultStr := ""
		if conversion.IsDefault {
			defaultStr = " DEFAULT"
		}
		metadataFile.MustPrintf("\n\nCREATE%s CONVERSION %s FOR '%s' TO '%s' FROM %s;",
			defaultStr, convFQN, conversion.ForEncoding, conversion.ToEncoding, conversion.ConversionFunction)

		section, entry := conversion.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, conversionMetadata[conversion.GetUniqueID()], conversion, "")
	}
}

func PrintCreateForeignDataWrapperStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC,
	fdw ForeignDataWrapper, funcInfoMap map[uint32]FunctionInfo, fdwMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE FOREIGN DATA WRAPPER %s", fdw.Name)

	if fdw.Handler != 0 {
		metadataFile.MustPrintf("\n\tHANDLER %s", funcInfoMap[fdw.Handler].QualifiedName)
	}
	if fdw.Validator != 0 {
		metadataFile.MustPrintf("\n\tVALIDATOR %s", funcInfoMap[fdw.Validator].QualifiedName)
	}
	if fdw.Options != "" {
		metadataFile.MustPrintf("\n\tOPTIONS (%s)", fdw.Options)
	}
	metadataFile.MustPrintf(";")

	section, entry := fdw.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, fdwMetadata, fdw, "")
}

func PrintCreateServerStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, server ForeignServer, serverMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE SERVER %s", server.Name)
	if server.Type != "" {
		metadataFile.MustPrintf("\n\tTYPE '%s'", server.Type)
	}
	if server.Version != "" {
		metadataFile.MustPrintf("\n\tVERSION '%s'", server.Version)
	}
	metadataFile.MustPrintf("\n\tFOREIGN DATA WRAPPER %s", server.ForeignDataWrapper)
	if server.Options != "" {
		metadataFile.MustPrintf("\n\tOPTIONS (%s)", server.Options)
	}
	metadataFile.MustPrintf(";")

	//NOTE: We must specify SERVER when creating and dropping, but FOREIGN SERVER when granting and revoking
	section, entry := server.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, serverMetadata, server, "")
}

func PrintCreateUserMappingStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, mapping UserMapping) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE USER MAPPING FOR %s\n\tSERVER %s", mapping.User, mapping.Server)
	if mapping.Options != "" {
		metadataFile.MustPrintf("\n\tOPTIONS (%s)", mapping.Options)
	}
	metadataFile.MustPrintf(";")

	section, entry := mapping.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
}
