package integration

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("synchronized snapshot integration tests", func() {
	BeforeEach(func() {
		if connectionPool.Version.Before("6.21.0") {
			Skip("snapshot feature not supported")
		}
	})
	Describe("GetSynchronizedSnapshot", func() {
		It("returns a correctly formatted snapshot identifier", func() {
			snapshotId, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapshotId).ToNot(BeEmpty())
			Expect(snapshotId).To(MatchRegexp("[0-9a-fA-F]+-\\d"))
		})
		It("returns a valid snapshot identifier that can be used to set a snapshot", func() {
			exportConn := testutils.SetupTestDbConn("testdb")
			importConn := testutils.SetupTestDbConn("testdb")
			exportConn.MustBegin()
			importConn.MustBegin()
			snapshotId, err := backup.GetSynchronizedSnapshot(exportConn)
			Expect(err).ToNot(HaveOccurred())

			_, err = importConn.Exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotId), 0)
			Expect(err).ToNot(HaveOccurred())

			exportConn.MustCommit()
			importConn.MustCommit()
			exportConn.Close()
			importConn.Close()
		})
	})
	Describe("SetSynchronizedSnapshot", func() {
		It("sets snapshot to snapshotId", func() {
			var snapshotId string
			exportConn := testutils.SetupTestDbConn("testdb")
			importConn := testutils.SetupTestDbConn("testdb")
			exportConn.MustBegin()
			importConn.MustBegin()
			err := exportConn.Get(&snapshotId, "SELECT pg_catalog.pg_export_snapshot()", 0)
			Expect(err).ToNot(HaveOccurred())

			err = backup.SetSynchronizedSnapshot(importConn, 0, snapshotId)
			Expect(err).ToNot(HaveOccurred())

			exportConn.MustCommit()
			importConn.MustCommit()
			exportConn.Close()
			importConn.Close()
		})
	})
	Describe("functionality tests", func() {
		BeforeEach(func() {
			connectionPool.Exec("CREATE TABLE IF NOT EXISTS snapshot_test (a int)")
			connectionPool.Exec("TRUNCATE TABLE snapshot_test")
		})
		It("handles concurrent insert", func() {
			query := "SELECT count(*) FROM snapshot_test"
			var result int
			exportConn := testutils.SetupTestDbConn("testdb")
			importConn := testutils.SetupTestDbConn("testdb")
			connectionPool.Exec("INSERT INTO snapshot_test values(1)")

			exportConn.MustBegin(0)
			snapshotId, err := backup.GetSynchronizedSnapshot(exportConn)
			Expect(err).ToNot(HaveOccurred())
			// External command inserts row
			connectionPool.Exec("INSERT INTO snapshot_test values(2)")
			connectionPool.Get(&result, query)
			Expect(result).To(Equal(2))

			// export should not see inserted row
			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(1))

			// import should see inserted row
			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(2))

			importConn.MustBegin(0)
			err = backup.SetSynchronizedSnapshot(importConn, 0, snapshotId)
			// Should see snapshot with 1 row
			Expect(err).ToNot(HaveOccurred())
			importConn.Get(&result, query)
			Expect(result).To(Equal(1))

			// after commits, inserted row is visible to both conns
			err = exportConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())
			err = importConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())

			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(2))

			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(2))

			exportConn.Close()
			importConn.Close()
		})
		It("handles concurrent update", func() {
			var result int
			query := "SELECT count(*) from snapshot_test WHERE a=99"
			exportConn := testutils.SetupTestDbConn("testdb")
			importConn := testutils.SetupTestDbConn("testdb")
			connectionPool.Exec("INSERT INTO snapshot_test SELECT a FROM generate_series(1,5) a")

			// export the snapshot
			exportConn.MustBegin(0)
			snapshotId, err := backup.GetSynchronizedSnapshot(exportConn)
			Expect(err).ToNot(HaveOccurred())
			Expect(snapshotId).ToNot(BeEmpty())

			// external command updates row
			_, err = connectionPool.Exec("UPDATE snapshot_test SET a=99 WHERE a=1")
			Expect(err).ToNot(HaveOccurred())
			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(1))

			// export should not see updated row
			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(0))

			// import should see updated row
			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(1))

			// now set snapshot, import should not see updated row
			importConn.MustBegin(0)
			err = backup.SetSynchronizedSnapshot(importConn, 0, snapshotId)
			Expect(err).ToNot(HaveOccurred())
			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(0))

			// after commits, updated row is visible to both conns
			err = exportConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())
			err = importConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())

			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(1))

			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(1))

			exportConn.Close()
			importConn.Close()
		})
		It("handles concurrent delete", func() {
			query := "SELECT count(*) FROM snapshot_test"
			var result int
			exportConn := testutils.SetupTestDbConn("testdb")
			importConn := testutils.SetupTestDbConn("testdb")
			connectionPool.Exec("INSERT INTO snapshot_test SELECT a FROM generate_series(1,5) a")

			exportConn.MustBegin(0)
			snapshotId, err := backup.GetSynchronizedSnapshot(exportConn)
			Expect(err).ToNot(HaveOccurred())
			// External command deletes row
			connectionPool.Exec("DELETE FROM snapshot_test where a=1")
			connectionPool.Get(&result, query)
			Expect(result).To(Equal(4))

			// export should see all 5 rows
			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(5))

			// import should see 4 rows
			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(4))

			importConn.MustBegin(0)
			err = backup.SetSynchronizedSnapshot(importConn, 0, snapshotId)
			// Should see snapshot with 5 rows
			Expect(err).ToNot(HaveOccurred())
			importConn.Get(&result, query)
			Expect(result).To(Equal(5))

			// after commits, deleted row is not visible to either conn
			err = exportConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())
			err = importConn.Commit(0)
			Expect(err).ToNot(HaveOccurred())

			err = exportConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(4))

			err = importConn.Get(&result, query)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(4))

			exportConn.Close()
			importConn.Close()
		})
	})
})
