package backup_test

import (
	"database/sql/driver"
	"fmt"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/queries_acl tests", func() {
	Describe("GetMetadataForObjectType", func() {
		var params backup.MetadataQueryParams
		header := []string{"oid", "privileges", "owner", "comment"}
		emptyRows := sqlmock.NewRows(header)

		getSecurityLabelReplace := func() (string, string, string) {
			securityLabelSelectReplace, securityLabelJoinReplace, sharedSecurityLabelJoinReplace := "", "", ""
			if connectionPool.Version.AtLeast("6") {
				securityLabelSelectReplace = `
		coalesce(sec.label,'') AS securitylabel,
		coalesce(sec.provider, '') AS securitylabelprovider,`
				securityLabelJoinReplace = `
		LEFT JOIN pg_seclabel sec ON (sec.objoid = o.oid AND sec.classoid = 'table'::regclass AND sec.objsubid = 0)`
				sharedSecurityLabelJoinReplace = `
		LEFT JOIN pg_shseclabel sec ON (sec.objoid = o.oid AND sec.classoid = 'table'::regclass)`
			}

			return securityLabelSelectReplace, securityLabelJoinReplace, sharedSecurityLabelJoinReplace
		}

		BeforeEach(func() {
			params = backup.MetadataQueryParams{ObjectType: toc.OBJ_RELATION, NameField: "name", OwnerField: "owner", CatalogTable: "table"}
		})
		It("queries metadata for an object with default params", func() {
			securityLabelSelectReplace, securityLabelJoinReplace, _ := getSecurityLabelReplace()

			mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`SELECT
		'RELATION' AS objecttype,
		'table'::regclass::oid AS classid,
		o.oid,
		coalesce(quote_ident(name),'') AS name,
		'' AS kind,
		coalesce(quote_ident(''),'') AS schema,
		quote_ident(pg_get_userbyid(owner)) AS owner,
		'' AS privileges,%s
		coalesce(description,'') AS comment
	FROM table o
		LEFT JOIN pg_description d ON (d.objoid = o.oid AND d.classoid = 'table'::regclass AND d.objsubid = 0)%s
	WHERE 1 = 1
	ORDER BY o.oid`, securityLabelSelectReplace, securityLabelJoinReplace))).WillReturnRows(emptyRows)
			backup.GetMetadataForObjectType(connectionPool, params)
		})
		It("queries metadata for an object with a schema field", func() {
			securityLabelSelectReplace, securityLabelJoinReplace, _ := getSecurityLabelReplace()

			mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`
	SELECT 'RELATION' AS objecttype,
		'table'::regclass::oid AS classid,
		o.oid,
		coalesce(quote_ident(name),'') AS name,
		'' AS kind,
		coalesce(quote_ident(n.nspname),'') AS schema,
		quote_ident(pg_get_userbyid(owner)) AS owner,
		'' AS privileges,%s
		coalesce(description,'') AS comment
	FROM table o
		LEFT JOIN pg_description d ON (d.objoid = o.oid AND d.classoid = 'table'::regclass AND d.objsubid = 0)
		JOIN pg_namespace n ON o.schema = n.oid%s
	WHERE n.nspname NOT LIKE 'pg_temp_%%' AND n.nspname NOT LIKE 'pg_toast%%' AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog')
	ORDER BY o.oid`, securityLabelSelectReplace, securityLabelJoinReplace))).WillReturnRows(emptyRows)
			params.SchemaField = "schema"
			backup.GetMetadataForObjectType(connectionPool, params)
		})
		It("queries metadata for an object with an ACL field", func() {
			securityLabelSelectReplace, securityLabelJoinReplace, _ := getSecurityLabelReplace()
			aclLateralJoin := ""
			aclCols := ""
			if connectionPool.Version.AtLeast("7") {
				aclLateralJoin = fmt.Sprintf(
					`LEFT JOIN LATERAL unnest(o.acl) ljl_unnest ON o.acl IS NOT NULL AND array_length(o.acl, 1) != 0`)
				aclCols = "ljl_unnest"
			} else {
				aclCols = fmt.Sprintf(`CASE
			WHEN acl IS NULL THEN NULL
			WHEN array_upper(acl, 1) = 0 THEN acl[0]
			ELSE unnest(acl) END`)
			}

			mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`
	SELECT 'RELATION' AS objecttype,
		'table'::regclass::oid AS classid,
		o.oid,
		coalesce(quote_ident(name),'') AS name,
		CASE
			WHEN acl IS NULL THEN ''
			WHEN array_upper(acl, 1) = 0 THEN 'Empty'
			ELSE '' END AS kind,
		coalesce(quote_ident(''),'') AS schema,
		quote_ident(pg_get_userbyid(owner)) AS owner,
		%s AS privileges,%s
		coalesce(description,'') AS comment
	FROM table o
		LEFT JOIN pg_description d ON (d.objoid = o.oid AND d.classoid = 'table'::regclass AND d.objsubid = 0)%s
	%s
	WHERE 1 = 1
	ORDER BY o.oid`, aclCols, securityLabelSelectReplace, securityLabelJoinReplace, aclLateralJoin))).WillReturnRows(emptyRows)
			params.ACLField = "acl"
			backup.GetMetadataForObjectType(connectionPool, params)
		})
		It("queries metadata for a shared object", func() {
			securityLabelSelectReplace, _, sharedSecurityLabelJoinReplace := getSecurityLabelReplace()

			mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`
	SELECT 'RELATION' AS objecttype,
		'table'::regclass::oid AS classid,
		o.oid,
		coalesce(quote_ident(name),'') AS name,
		'' AS kind,
		coalesce(quote_ident(''),'') AS schema,
		quote_ident(pg_get_userbyid(owner)) AS owner,
		'' AS privileges,%s
		coalesce(description,'') AS comment
	FROM table o
		LEFT JOIN pg_shdescription d ON (d.objoid = o.oid AND d.classoid = 'table'::regclass)%s
	WHERE 1 = 1
	ORDER BY o.oid`, securityLabelSelectReplace, sharedSecurityLabelJoinReplace))).WillReturnRows(emptyRows)
			params.Shared = true
			backup.GetMetadataForObjectType(connectionPool, params)
		})
		It("returns metadata for multiple objects", func() {
			aclRowOne := []driver.Value{"1", "gpadmin=a/gpadmin", "testrole", ""}
			aclRowTwo := []driver.Value{"1", "testrole=a/gpadmin", "testrole", ""}
			commentRow := []driver.Value{"2", "", "testrole", "This is a metadata comment."}
			fakeRows := sqlmock.NewRows(header).AddRow(aclRowOne...).AddRow(aclRowTwo...).AddRow(commentRow...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			params.ACLField = "acl"
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, params)

			expectedOne := backup.ObjectMetadata{Privileges: []backup.ACL{
				{Grantee: "gpadmin", Insert: true},
				{Grantee: "testrole", Insert: true},
			}, Owner: "testrole"}
			expectedTwo := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole", Comment: "This is a metadata comment."}
			resultOne := resultMetadataMap[backup.UniqueID{Oid: 1}]
			resultTwo := resultMetadataMap[backup.UniqueID{Oid: 2}]
			Expect(resultMetadataMap).To(HaveLen(2))
			structmatcher.ExpectStructsToMatch(&expectedOne, &resultOne)
			structmatcher.ExpectStructsToMatch(&expectedTwo, &resultTwo)
		})
	})
	Describe("GetCommentsForObjectType", func() {
		var params backup.MetadataQueryParams
		header := []string{"oid", "comment"}
		emptyRows := sqlmock.NewRows(header)

		BeforeEach(func() {
			params = backup.MetadataQueryParams{NameField: "name", OidField: "oid", CatalogTable: "table"}
		})
		It("returns comment for object with default params", func() {
			mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT 'table'::regclass::oid AS classid,
		o.oid AS oid,
		coalesce(description,'') AS comment
	FROM table o JOIN pg_description d ON (d.objoid = oid AND d.classoid = 'table'::regclass AND d.objsubid = 0)`)).WillReturnRows(emptyRows)
			backup.GetCommentsForObjectType(connectionPool, params)
		})
		It("returns comment for object with different comment table", func() {
			params.CommentTable = "comment_table"
			mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT 'table'::regclass::oid AS classid,
		o.oid AS oid,
		coalesce(description,'') AS comment
	FROM table o JOIN pg_description d ON (d.objoid = oid AND d.classoid = 'comment_table'::regclass AND d.objsubid = 0)`)).WillReturnRows(emptyRows)
			backup.GetCommentsForObjectType(connectionPool, params)
		})
		It("returns comment for a shared object", func() {
			params.Shared = true
			mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT 'table'::regclass::oid AS classid,
		o.oid AS oid,
		coalesce(description,'') AS comment
	FROM table o JOIN pg_shdescription d ON (d.objoid = oid AND d.classoid = 'table'::regclass)`)).WillReturnRows(emptyRows)
			backup.GetCommentsForObjectType(connectionPool, params)
		})
		It("returns comments for multiple objects", func() {
			rowOne := []driver.Value{"1", "This is a metadata comment."}
			rowTwo := []driver.Value{"2", "This is also a metadata comment."}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, params)

			expectedOne := backup.ObjectMetadata{Privileges: []backup.ACL{}, Comment: "This is a metadata comment."}
			expectedTwo := backup.ObjectMetadata{Privileges: []backup.ACL{}, Comment: "This is also a metadata comment."}
			resultOne := resultMetadataMap[backup.UniqueID{Oid: 1}]
			resultTwo := resultMetadataMap[backup.UniqueID{Oid: 2}]
			Expect(resultMetadataMap).To(HaveLen(2))
			structmatcher.ExpectStructsToMatch(&expectedOne, &resultOne)
			structmatcher.ExpectStructsToMatch(&expectedTwo, &resultTwo)
		})
	})
})
