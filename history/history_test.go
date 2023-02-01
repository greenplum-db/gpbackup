package history_test

import (
	"os"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	testLogfile *Buffer
)

func TestBackupHistory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "History Suite")
}

var _ = BeforeSuite(func() {
	_, _, testLogfile = testhelper.SetupTestLogger()
})

var _ = Describe("backup/history tests", func() {
	var testConfig1, testConfig2, testConfigMalformed history.BackupConfig
	var historyDBPath = "/tmp/history_db.db"

	BeforeEach(func() {
		testConfig1 = history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestamp1",
		}
		testConfigMalformed = history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{{"differentTimestamp", []string{"testschema.testtable1"}}},
			Timestamp:        "timestamp1",
		}
		testConfig2 = history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{{"timestamp1", []string{"testschema.testtable1"}}, {"timestamp2", []string{"testschema.testtable2"}}},
			Timestamp:        "timestamp2",
		}
		_ = os.Remove(historyDBPath)
	})

	AfterEach(func() {
		_ = os.Remove(historyDBPath)
	})
	Describe("CurrentTimestamp", func() {
		It("returns the current timestamp", func() {
			operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
			expected := "20170101010101"
			actual := history.CurrentTimestamp()
			Expect(actual).To(Equal(expected))
		})
	})
	Describe("InitializeHistoryDatabase", func() {
		It("creates, initializes, and returns a handle to the database if none is already present", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			tablesRow, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' order by name;")
			Expect(err).To(BeNil())

			var tableNames []string
			for tablesRow.Next() {
				var exclSchema string
				err = tablesRow.Scan(&exclSchema)
				Expect(err).To(BeNil())
				tableNames = append(tableNames, exclSchema)
			}

			Expect(tableNames[0]).To(Equal("backups"))
			Expect(tableNames[1]).To(Equal("exclude_relations"))
			Expect(tableNames[2]).To(Equal("exclude_schemas"))
			Expect(tableNames[3]).To(Equal("include_relations"))
			Expect(tableNames[4]).To(Equal("include_schemas"))
			Expect(tableNames[5]).To(Equal("restore_plans"))

		})

		It("returns a handle to an existing database if one is already present", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			createDummyTable := "CREATE TABLE IF NOT EXISTS dummy (dummy int);"
			_, _ = db.Exec(createDummyTable)
			db.Close()

			sameDB, _ := history.InitializeHistoryDatabase(historyDBPath)
			tableRow := sameDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' and name='dummy';")

			var tableName string
			err := tableRow.Scan(&tableName)
			Expect(err).To(BeNil())
			Expect(tableName).To(Equal("dummy"))

		})
	})

	Describe("StoreBackupHistory", func() {
		It("stores a config into the database", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			err := history.StoreBackupHistory(db, &testConfig1)
			Expect(err).To(BeNil())

			tableRow := db.QueryRow("SELECT timestamp, database_name FROM backups;")

			var timestamp string
			var dbName string
			err = tableRow.Scan(&timestamp, &dbName)
			Expect(err).To(BeNil())
			Expect(timestamp).To(Equal(testConfig1.Timestamp))
			Expect(dbName).To(Equal(testConfig1.DatabaseName))

			inclRelRows, err := db.Query("SELECT timestamp, name FROM include_relations ORDER BY name")
			Expect(err).To(BeNil())
			var includeRelations []string
			for inclRelRows.Next() {
				var inclRelTS string
				var inclRel string
				err = inclRelRows.Scan(&inclRelTS, &inclRel)
				Expect(err).To(BeNil())
				Expect(inclRelTS).To(Equal(timestamp))
				includeRelations = append(includeRelations, inclRel)
			}

			Expect(includeRelations[0]).To(Equal("testschema.testtable1"))
			Expect(includeRelations[1]).To(Equal("testschema.testtable2"))
		})

		It("refuses to store a config into the database if the timestamp is already present", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			err := history.StoreBackupHistory(db, &testConfig1)
			Expect(err).To(BeNil())

			err = history.StoreBackupHistory(db, &testConfig1)
			Expect(err.Error()).To(Equal("UNIQUE constraint failed: backups.timestamp"))
		})

		It("refuses to store a config into the database if the config is malformed", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			err := history.StoreBackupHistory(db, &testConfigMalformed)
			Expect(err.Error()).To(Equal("FOREIGN KEY constraint failed"))
		})
	})

	Describe("GetBackupConfig", func() {
		It("gets a config from the database", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			defer db.Close()
			err := history.StoreBackupHistory(db, &testConfig1)
			Expect(err).To(BeNil())

			config, err := history.GetBackupConfig(testConfig1.Timestamp, db)
			Expect(err).To(BeNil())
			Expect(config).To(structmatcher.MatchStruct(testConfig1))
		})

		It("refuses to get a config from the database if the timestamp is not present", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			defer db.Close()
			err := history.StoreBackupHistory(db, &testConfig1)
			Expect(err).To(BeNil())

			_, err = history.GetBackupConfig("timestampDNE", db)
			Expect(err.Error()).To(Equal("timestamp doesn't match any existing backups"))

		})
		It("gets a config from the database with multiple restore plan entries", func() {
			db, _ := history.InitializeHistoryDatabase(historyDBPath)
			defer db.Close()
			err := history.StoreBackupHistory(db, &testConfig1)
			Expect(err).To(BeNil())
			err = history.StoreBackupHistory(db, &testConfig2)
			Expect(err).To(BeNil())

			config, err := history.GetBackupConfig(testConfig2.Timestamp, db)
			Expect(err).To(BeNil())
			Expect(config).To(structmatcher.MatchStruct(testConfig2))
		})
	})
})
