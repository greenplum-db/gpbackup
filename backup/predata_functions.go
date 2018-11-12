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
	funcFQN := utils.MakeFQN(funcDef.Schema, funcDef.Name)
	currStatement := fmt.Sprintf("CREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
	currStatement += fmt.Sprintf("%s AS", funcDef.ResultType)
	currStatement += PrintFunctionBodyOrPath(funcDef)
	currStatement += fmt.Sprintf("LANGUAGE %s", funcDef.Language)
	currStatement += PrintFunctionModifiers(funcDef)
	currStatement += ";"

	PrintStatements(metadataFile, toc, funcDef, []string{currStatement})
	nameStr := fmt.Sprintf("%s(%s)", funcFQN, funcDef.IdentArgs)
	NewPrintObjectMetadata(metadataFile, toc, funcMetadata, funcDef, nameStr)
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(funcDef Function) string {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */

	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		return fmt.Sprintf("\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		return fmt.Sprintf("\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(funcDef Function) string {
	funcMods := ""
	switch funcDef.DataAccess {
	case "c":
		funcMods += fmt.Sprintf(" CONTAINS SQL")
	case "m":
		funcMods += fmt.Sprintf(" MODIFIES SQL DATA")
	case "n":
		funcMods += fmt.Sprintf(" NO SQL")
	case "r":
		funcMods += fmt.Sprintf(" READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		funcMods += fmt.Sprintf(" IMMUTABLE")
	case "s":
		funcMods += fmt.Sprintf(" STABLE")
	case "v": // Default case, don't print anything else
	}
	switch funcDef.ExecLocation {
	case "m":
		funcMods += fmt.Sprintf(" EXECUTE ON MASTER")
	case "s":
		funcMods += fmt.Sprintf(" EXECUTE ON ALL SEGMENTS")
	case "a": // Default case, don't print anything else
	}
	if funcDef.IsWindow {
		funcMods += fmt.Sprintf(" WINDOW")
	}
	if funcDef.IsStrict {
		funcMods += fmt.Sprintf(" STRICT")
	}
	if funcDef.IsLeakProof {
		funcMods += fmt.Sprintf(" LEAKPROOF")
	}
	if funcDef.IsSecurityDefiner {
		funcMods += fmt.Sprintf(" SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1) || funcDef.Cost == 0) {
		funcMods += fmt.Sprintf("\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		funcMods += fmt.Sprintf("\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		funcMods += fmt.Sprintf("\n%s", funcDef.Config)
	}
	return funcMods
}

func PrintCreateAggregateStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, aggDef Aggregate, funcInfoMap map[uint32]FunctionInfo, aggMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	aggFQN := utils.MakeFQN(aggDef.Schema, aggDef.Name)
	orderedStr := ""
	if aggDef.IsOrdered {
		orderedStr = "ORDERED "
	}
	argumentsStr := "*"
	if aggDef.Arguments != "" {
		argumentsStr = aggDef.Arguments
	}
	metadataFile.MustPrintf("\n\nCREATE %sAGGREGATE %s(%s) (\n", orderedStr, aggFQN, argumentsStr)

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

	identArgumentsStr := "*"
	if aggDef.IdentArgs != "" {
		identArgumentsStr = aggDef.IdentArgs
	}
	aggFQN = fmt.Sprintf("%s(%s)", aggFQN, identArgumentsStr)
	aggWithArgs := fmt.Sprintf("%s(%s)", aggDef.Name, identArgumentsStr)
	PrintObjectMetadata(metadataFile, aggMetadata, aggFQN, "AGGREGATE")
	toc.AddPredataEntry(aggDef.Schema, aggWithArgs, "AGGREGATE", "", start, metadataFile)
}

func PrintCreateCastStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, castDef Cast, castMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	castStr := fmt.Sprintf("(%s AS %s)", castDef.SourceTypeFQN, castDef.TargetTypeFQN)
	metadataFile.MustPrintf("\n\nCREATE CAST %s\n", castStr)
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
	PrintObjectMetadata(metadataFile, castMetadata, castStr, "CAST")
	filterSchema := "pg_catalog"
	if castDef.CastMethod == "f" {
		filterSchema = castDef.FunctionSchema // Use the function's schema to allow restore filtering
	}
	toc.AddPredataEntry(filterSchema, castStr, "CAST", "", start, metadataFile)
}

func PrintCreateExtensionStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, extensionDefs []Extension, extensionMetadata MetadataMap) {
	for _, extensionDef := range extensionDefs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nSET search_path=%s,pg_catalog;\nCREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\nSET search_path=pg_catalog;", extensionDef.Schema, extensionDef.Name, extensionDef.Schema)
		PrintObjectMetadata(metadataFile, extensionMetadata[extensionDef.GetUniqueID()], extensionDef.Name, "EXTENSION")
		toc.AddPredataEntry("", extensionDef.Name, "EXTENSION", "", start, metadataFile)
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
			isLangFuncMap[funcDef.Oid] = (isLangFuncMap[funcDef.Oid] || funcDef.Oid == procLang.Handler ||
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
		metadataFile.MustPrintf(alterStr)
		PrintObjectMetadata(metadataFile, procLangMetadata[procLang.GetUniqueID()], procLang.Name, "LANGUAGE")
		metadataFile.MustPrintln()
		toc.AddPredataEntry("", procLang.Name, "PROCEDURAL LANGUAGE", "", start, metadataFile)
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
		PrintObjectMetadata(metadataFile, conversionMetadata[conversion.GetUniqueID()], convFQN, "CONVERSION")
		metadataFile.MustPrintln()
		toc.AddPredataEntry(conversion.Schema, conversion.Name, "CONVERSION", "", start, metadataFile)
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
	PrintObjectMetadata(metadataFile, fdwMetadata, fdw.Name, "FOREIGN DATA WRAPPER")
	toc.AddPredataEntry("", fdw.Name, "FOREIGN DATA WRAPPER", "", start, metadataFile)
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
	PrintObjectMetadata(metadataFile, serverMetadata, server.Name, "FOREIGN SERVER")
	toc.AddPredataEntry("", server.Name, "FOREIGN SERVER", "", start, metadataFile)
}

func PrintCreateUserMappingStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, mapping UserMapping) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE USER MAPPING FOR %s\n\tSERVER %s", mapping.User, mapping.Server)
	if mapping.Options != "" {
		metadataFile.MustPrintf("\n\tOPTIONS (%s)", mapping.Options)
	}
	metadataFile.MustPrintf(";")
	// User mappings don't have a unique name, so we construct an arbitrary identifier
	mappingStr := fmt.Sprintf("%s ON %s", mapping.User, mapping.Server)
	toc.AddPredataEntry("", mappingStr, "USER MAPPING", "", start, metadataFile)
}
