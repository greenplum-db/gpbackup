package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_textsearch.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

type TextSearchParser struct {
	Oid          uint32
	Schema       string
	Name         string
	StartFunc    string
	TokenFunc    string
	EndFunc      string
	LexTypesFunc string
	HeadlineFunc string
}

func (tsp TextSearchParser) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          tsp.Schema,
			Name:            tsp.Name,
			ObjectType:      toc.OBJ_TEXT_SEARCH_PARSER,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (tsp TextSearchParser) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TS_PARSER_OID, Oid: tsp.Oid}
}

func (tsp TextSearchParser) FQN() string {
	return utils.MakeFQN(tsp.Schema, tsp.Name)
}

func GetTextSearchParsers(connectionPool *dbconn.DBConn) []TextSearchParser {
	query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(nspname) AS schema,
		quote_ident(prsname) AS name,
		prsstart::regproc::text AS startfunc,
		prstoken::regproc::text AS tokenfunc,
		prsend::regproc::text AS endfunc,
		prslextype::regproc::text AS lextypesfunc,
		CASE
			WHEN prsheadline::regproc::text = '-'
			THEN '' ELSE prsheadline::regproc::text
		END AS headlinefunc 
	FROM pg_ts_parser p
		JOIN pg_namespace n ON n.oid = p.prsnamespace
	WHERE %s
		AND %s
	ORDER BY prsname`, SchemaFilterClause("n"), ExtensionFilterClause("p"))

	results := make([]TextSearchParser, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type TextSearchTemplate struct {
	Oid        uint32
	Schema     string
	Name       string
	InitFunc   string
	LexizeFunc string
}

func (tst TextSearchTemplate) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          tst.Schema,
			Name:            tst.Name,
			ObjectType:      toc.OBJ_TEXT_SEARCH_TEMPLATE,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (tst TextSearchTemplate) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TS_TEMPLATE_OID, Oid: tst.Oid}
}

func (tst TextSearchTemplate) FQN() string {
	return utils.MakeFQN(tst.Schema, tst.Name)
}

func GetTextSearchTemplates(connectionPool *dbconn.DBConn) []TextSearchTemplate {
	query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(nspname) as schema,
		quote_ident(tmplname) AS name,
		CASE
			WHEN tmplinit::regproc::text = '-'
			THEN '' ELSE tmplinit::regproc::text
		END AS initfunc,
		tmpllexize::regproc::text AS lexizefunc
	FROM pg_ts_template p
		JOIN pg_namespace n ON n.oid = p.tmplnamespace
	WHERE %s
		AND %s
	ORDER BY tmplname`,
		SchemaFilterClause("n"), ExtensionFilterClause("p"))

	results := make([]TextSearchTemplate, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type TextSearchDictionary struct {
	Oid        uint32
	Schema     string
	Name       string
	Template   string
	InitOption string
}

func (tsd TextSearchDictionary) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          tsd.Schema,
			Name:            tsd.Name,
			ObjectType:      toc.OBJ_TEXT_SEARCH_DICTIONARY,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (tsd TextSearchDictionary) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TS_DICT_OID, Oid: tsd.Oid}
}

func (tsd TextSearchDictionary) FQN() string {
	return utils.MakeFQN(tsd.Schema, tsd.Name)
}

func GetTextSearchDictionaries(connectionPool *dbconn.DBConn) []TextSearchDictionary {
	query := fmt.Sprintf(`
	SELECT d.oid,
		quote_ident(dict_ns.nspname) as schema,
		quote_ident(dictname) AS name,
		quote_ident(tmpl_ns.nspname) || '.' || quote_ident(t.tmplname) AS template,
		COALESCE(dictinitoption, '') AS initoption
	FROM pg_ts_dict d
		JOIN pg_ts_template t ON t.oid = d.dicttemplate
		JOIN pg_namespace tmpl_ns ON tmpl_ns.oid = t.tmplnamespace
		JOIN pg_namespace dict_ns ON dict_ns.oid = d.dictnamespace
	WHERE %s
		AND %s
	ORDER BY dictname`,
		SchemaFilterClause("dict_ns"), ExtensionFilterClause("d"))

	results := make([]TextSearchDictionary, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type TextSearchConfiguration struct {
	Oid          uint32
	Schema       string
	Name         string
	Parser       string
	TokenToDicts map[string][]string
}

func (tsc TextSearchConfiguration) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          tsc.Schema,
			Name:            tsc.Name,
			ObjectType:      toc.OBJ_TEXT_SEARCH_CONFIGURATION,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (tsc TextSearchConfiguration) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TS_CONFIG_OID, Oid: tsc.Oid}
}

func (tsc TextSearchConfiguration) FQN() string {
	return utils.MakeFQN(tsc.Schema, tsc.Name)
}

func GetTextSearchConfigurations(connectionPool *dbconn.DBConn) []TextSearchConfiguration {
	query := fmt.Sprintf(`
	SELECT c.oid AS configoid,
		quote_ident(cfg_ns.nspname) AS schema,
		quote_ident(cfgname) AS name,
		cfgparser AS parseroid,
		quote_ident(prs_ns.nspname) || '.' || quote_ident(prsname) AS parserfqn
	FROM pg_ts_config c
		JOIN pg_ts_parser p ON p.oid = c.cfgparser
		JOIN pg_namespace cfg_ns ON cfg_ns.oid = c.cfgnamespace
		JOIN pg_namespace prs_ns ON prs_ns.oid = prsnamespace
	WHERE %s
		AND %s
	ORDER BY cfgname`,
		SchemaFilterClause("cfg_ns"), ExtensionFilterClause("c"))

	results := make([]struct {
		Schema    string
		Name      string
		ConfigOid uint32
		ParserOid uint32
		ParserFQN string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	parserTokens := NewParserTokenTypes()
	typeMappings := getTypeMappings(connectionPool)

	configurations := make([]TextSearchConfiguration, 0)
	for _, row := range results {
		config := TextSearchConfiguration{}
		config.Oid = row.ConfigOid
		config.Schema = row.Schema
		config.Name = row.Name
		config.Parser = row.ParserFQN
		config.TokenToDicts = make(map[string][]string)
		for _, mapping := range typeMappings[row.ConfigOid] {
			tokenName := parserTokens.TokenName(connectionPool, row.ParserOid, mapping.TokenType)
			config.TokenToDicts[tokenName] = append(config.TokenToDicts[tokenName], mapping.Dictionary)
		}

		configurations = append(configurations, config)
	}

	return configurations
}

type ParserTokenType struct {
	TokenID uint32
	Alias   string
}

type ParserTokenTypes struct {
	forParser map[uint32][]ParserTokenType
}

func NewParserTokenTypes() *ParserTokenTypes {
	return &ParserTokenTypes{map[uint32][]ParserTokenType{}}
}

func (tokenTypes *ParserTokenTypes) TokenName(connectionPool *dbconn.DBConn, parserOid uint32, tokenTypeID uint32) string {
	typesForParser, ok := tokenTypes.forParser[parserOid]
	if !ok {
		typesForParser = make([]ParserTokenType, 0)
		query := fmt.Sprintf("SELECT tokid AS tokenid, alias FROM pg_catalog.ts_token_type('%d'::pg_catalog.oid)", parserOid)
		err := connectionPool.Select(&typesForParser, query)
		gplog.FatalOnError(err)

		tokenTypes.forParser[parserOid] = typesForParser
	}
	for _, token := range typesForParser {
		if token.TokenID == tokenTypeID {
			return token.Alias
		}
	}
	return ""
}

type TypeMapping struct {
	ConfigOid  uint32 `db:"mapcfg"`
	TokenType  uint32 `db:"maptokentype"`
	Dictionary string `db:"mapdictname"`
}

func getTypeMappings(connectionPool *dbconn.DBConn) map[uint32][]TypeMapping {
	query := `
	SELECT mapcfg,
		maptokentype,
		mapdict::pg_catalog.regdictionary AS mapdictname
	FROM pg_ts_config_map m`
	rows := make([]TypeMapping, 0)
	err := connectionPool.Select(&rows, query)
	gplog.FatalOnError(err)

	mapping := make(map[uint32][]TypeMapping)
	for _, row := range rows {
		mapping[row.ConfigOid] = append(mapping[row.ConfigOid], row)
	}
	return mapping
}
