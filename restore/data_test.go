package restore_test

import (
	"regexp"

	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/data tests", func() {
	Describe("CopyTableIn", func() {
		It("will restore a table from its own file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c -1", DecompressCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'gzip -d -c < <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, false, 3456, 0)
		})
		It("will restore a table from its own file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, false, 3456, 0)
		})
		It("will restore a table from a single data file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c -1", DecompressCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'set -o pipefail; gzip -d -c <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101.gz | $GPHOME/bin/gpbackup_helper --restore --toc-file=<SEG_DATA_DIR>/gpbackup_<SEGID>_20170101010101_toc.yaml --oid=2 --content=<SEGID> || test $? -eq 141' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101.gz"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, true, 2, 0)
		})
		It("will restore a table from a single data file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM '$GPHOME/bin/gpbackup_helper --restore --toc-file=<SEG_DATA_DIR>/gpbackup_<SEGID>_20170101010101_toc.yaml --oid=2 --content=<SEGID> < <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, true, 2, 0)
		})
	})
})
