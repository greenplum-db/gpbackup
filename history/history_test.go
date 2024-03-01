package history_test

import (
	"os"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/types"
)

var (
	testLogfile        *Buffer
	PredataArg         = []string{"--section", "predata"}
	DataArg            = []string{"--section", "data"}
	PostdataArg        = []string{"--section", "postdata"}
	PredataDataArg     = []string{"--section", "predata", "--section", "data"}
	PredataPostdataArg = []string{"--section", "predata", "--section", "postdata"}
	DataPostdataArg    = []string{"--section", "data", "--section", "postdata"}
	AllArg             = []string{"--section", "predata", "--section", "data", "--section", "postdata"}
)

func TestBackupHistory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "History Suite")
}

var _ = BeforeSuite(func() {
	_, _, testLogfile = testhelper.SetupTestLogger()
})

var _ = Describe("backup/history tests", func() {
	var testConfig1, testConfig2 history.BackupConfig
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
			Sections:         history.Predata | history.Data | history.Postdata,
		}
		testConfig2 = history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{{"timestamp1", []string{"testschema.testtable1"}}, {"timestamp2", []string{"testschema.testtable2"}}},
			Timestamp:        "timestamp2",
			Sections:         history.Predata | history.Data | history.Postdata,
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
			Expect(tableNames[5]).To(Equal("restore_plan_tables"))
			Expect(tableNames[6]).To(Equal("restore_plans"))

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
	Describe("Sections", func() {
		var (
			err   error
			s     *history.Sections
			flags *pflag.FlagSet
		)
		BeforeEach(func() {
			s = history.NewSections()
			flags = defaultFlags()
		})
		Describe("Basic tests", func() {
			BeforeEach(func() {
				s.Set(history.Predata | history.Data | history.Postdata)
			})
			It("finds sections with Contains", func() {
				Expect(s.Contains(history.Predata)).To(BeTrue())
				Expect(s.Contains(history.Data)).To(BeTrue())
				Expect(s.Contains(history.Postdata)).To(BeTrue())
			})
			It("matches section exactly with Is", func() {
				Expect(s.Is(history.Predata | history.Data | history.Postdata)).To(BeTrue())
				s.Clear(history.Data)
				Expect(s.Is(history.Predata | history.Postdata)).To(BeTrue())
			})
			It("converts section to string", func() {
				Expect(s.ToString()).To(Equal("predata, data, postdata"))
			})
			It("converts section to string with empty section", func() {
				s.Clear(history.Predata | history.Data | history.Postdata)
				Expect(s.ToString()).To(Equal(""))
			})
			It("converts section from string", func() {
				Expect(s.FromString("predata", "data", "postdata")).To(Succeed())
				Expect(s.Is(history.Predata | history.Data | history.Postdata)).To(BeTrue())
			})
			It("converts section from string with invalid section", func() {
				Expect(s.FromString("invalid")).To(MatchError("Unrecognized section name: invalid"))
			})
			It("converts section from string with empty string", func() {
				Expect(s.FromString("")).To(MatchError("No sections provided"))
			})
		})
		DescribeTable("SetBackup",
			func(args []string, setSections history.Sections, unsetSections history.Sections, expected types.GomegaMatcher) {
				Expect(flags.Parse(args)).To(Succeed())
				err := s.SetBackup(flags)
				Expect(err).To(expected)
				if err == nil {
					Expect(s.Is(setSections)).To(BeTrue())
				}
			},
			// Flag combinations
			Entry("handles --section with no args", []string{"--section", ""}, history.Empty, history.Empty, MatchError("No section flags provided")),
			Entry("handles invalid section", []string{"--section", "invalid"}, history.Empty, history.Empty, MatchError("Unrecognized section name: invalid")),
			Entry("handles no section flags", []string{}, history.Predata|history.Data|history.Postdata, history.Empty, Not(HaveOccurred())),

			Entry("--section=predata", []string{"--section", "predata"}, history.Predata, history.Data|history.Postdata, Not(HaveOccurred())),
			Entry("--section=data", []string{"--section", "data"}, history.Data, history.Predata|history.Postdata, Not(HaveOccurred())),
			Entry("--section=postdata", []string{"--section", "postdata"}, history.Postdata, history.Predata|history.Data, Not(HaveOccurred())),

			// Test backward compatibility
			Entry("--metadata-only", []string{"--metadata-only"}, history.Predata|history.Postdata, history.Data, Not(HaveOccurred())),
			Entry("--section=data", []string{"--section=data"}, history.Data, history.Predata|history.Postdata, Not(HaveOccurred())),

			// Exclusive flags
			Entry("--section=predata --incremental", []string{"--section", "predata", "--incremental"}, history.Empty, history.Empty, MatchError("Cannot use --incremental without section: data")),
			Entry("--section=predata --leaf-partition-data", []string{"--section", "predata", "--leaf-partition-data"}, history.Empty, history.Empty, MatchError("Cannot use --leaf-partition-data without section: data")),
		)
		DescribeTable("SetRestore",
			func(args []string, configSections history.Sections, expected types.GomegaMatcher) {
				Expect(flags.Parse(args)).To(Succeed())
				err = s.SetRestore(flags, &history.BackupConfig{Sections: configSections})
				Expect(err).To(expected)
			},

			// exclusive flags
			Entry("--section=data --with-globals", []string{"--section", "data", "--with-globals"}, history.Predata|history.Data|history.Postdata, MatchError("Cannot use --with-globals without section: predata")),
			Entry("--section=data --create-db", []string{"--section", "data", "--create-db"}, history.Predata|history.Data|history.Postdata, MatchError("Cannot use --create-db without section: predata")),

			// --section=predata
			Entry("[predata] from [predata] backup", PredataArg, history.Predata, Not(HaveOccurred())),
			Entry("[predata] from [data] backup ", PredataArg, history.Data, MatchError("Cannot restore: [predata] from backup containing: [data]")),
			Entry("[predata] from [postdata] backup ", PredataArg, history.Postdata, MatchError("Cannot restore: [predata] from backup containing: [postdata]")),
			Entry("[predata] from [predata, data] backup ", PredataArg, history.Predata|history.Data, Not(HaveOccurred())),
			Entry("[predata] from [predata, postdata] backup ", PredataArg, history.Predata|history.Postdata, Not(HaveOccurred())),
			Entry("[predata] from [data, postdata] backup ", PredataArg, history.Data|history.Postdata, MatchError("Cannot restore: [predata] from backup containing: [data, postdata]")),
			Entry("[predata] from [predata, data, postdata] backup ", PredataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// -section=data
			Entry("[data] from [predata] backup ", DataArg, history.Predata, MatchError("Cannot restore: [data] from backup containing: [predata]")),
			Entry("[data] from [data] backup ", DataArg, history.Data, Not(HaveOccurred())),
			Entry("[data] from [postdata] backup ", DataArg, history.Postdata, MatchError("Cannot restore: [data] from backup containing: [postdata]")),
			Entry("[data] from [predata, data] backup ", DataArg, history.Data, Not(HaveOccurred())),
			Entry("[data] from [predata, postdata] backup ", DataArg, history.Predata|history.Postdata, MatchError("Cannot restore: [data] from backup containing: [predata, postdata]")),
			Entry("[data] from [data, postdata] backup ", DataArg, history.Data|history.Postdata, Not(HaveOccurred())),
			Entry("[data] from [predata, data, postdata] backup ", DataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// --section=postdata
			Entry("[postdata] from [predata] backup ", PostdataArg, history.Predata, MatchError("Cannot restore: [postdata] from backup containing: [predata]")),
			Entry("[postdata] from [data] backup ", PostdataArg, history.Data, MatchError("Cannot restore: [postdata] from backup containing: [data]")),
			Entry("[postdata] from [postdata] backup ", PostdataArg, history.Postdata, Not(HaveOccurred())),
			Entry("[postdata] from [predata, data] backup ", PostdataArg, history.Predata|history.Data, MatchError("Cannot restore: [postdata] from backup containing: [predata, data]")),
			Entry("[postdata] from [data, postdata] backup ", PostdataArg, history.Data|history.Postdata, Not(HaveOccurred())),
			Entry("[postdata] from [predata, data, postdata] backup ", PostdataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// --section=predata --section=data
			Entry("[predata, data] from [predata] backup ", PredataDataArg, history.Predata, MatchError("Cannot restore: [predata, data] from backup containing: [predata]")),
			Entry("[predata, data] from [data] backup ", PredataDataArg, history.Data, MatchError("Cannot restore: [predata, data] from backup containing: [data]")),
			Entry("[predata, data] from [postdata] backup ", PredataDataArg, history.Postdata, MatchError("Cannot restore: [predata, data] from backup containing: [postdata]")),
			Entry("[predata, data] from [predata, data] backup ", PredataDataArg, history.Predata|history.Data, Not(HaveOccurred())),
			Entry("[predata, data] from [predata, postdata] backup ", PredataDataArg, history.Predata|history.Postdata, MatchError("Cannot restore: [predata, data] from backup containing: [predata, postdata]")),
			Entry("[predata, data] from [data, postdata] backup ", PredataDataArg, history.Data|history.Postdata, MatchError("Cannot restore: [predata, data] from backup containing: [data, postdata]")),
			Entry("[predata, data] from [predata, data, postdata] backup ", PredataDataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// --section=predata --section=postdata
			Entry("[predata, postdata] from [predata] backup ", PredataPostdataArg, history.Predata, MatchError("Cannot restore: [predata, postdata] from backup containing: [predata]")),
			Entry("[predata, postdata] from [data] backup ", PredataPostdataArg, history.Data, MatchError("Cannot restore: [predata, postdata] from backup containing: [data]")),
			Entry("[predata, postdata] from [postdata] backup ", PredataPostdataArg, history.Postdata, MatchError("Cannot restore: [predata, postdata] from backup containing: [postdata]")),
			Entry("[predata, postdata] from [predata, data] backup ", PredataPostdataArg, history.Predata|history.Data, MatchError("Cannot restore: [predata, postdata] from backup containing: [predata, data]")),
			Entry("[predata, postdata] from [predata, postdata] backup ", PredataPostdataArg, history.Predata|history.Postdata, Not(HaveOccurred())),
			Entry("[predata, postdata] from [data, postdata] backup ", PredataPostdataArg, history.Data|history.Postdata, MatchError("Cannot restore: [predata, postdata] from backup containing: [data, postdata]")),
			Entry("[predata, postdata] from [predata, data, postdata] backup ", PredataPostdataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// --section=data --section=postdata
			Entry("[data, postdata] from [predata] backup ", DataPostdataArg, history.Predata, MatchError("Cannot restore: [data, postdata] from backup containing: [predata]")),
			Entry("[data, postdata] from [data] backup ", DataPostdataArg, history.Data, MatchError("Cannot restore: [data, postdata] from backup containing: [data]")),
			Entry("[data, postdata] from [postdata] backup ", DataPostdataArg, history.Postdata, MatchError("Cannot restore: [data, postdata] from backup containing: [postdata]")),
			Entry("[data, postdata] from [predata, data] backup ", DataPostdataArg, history.Predata|history.Data, MatchError("Cannot restore: [data, postdata] from backup containing: [predata, data]")),
			Entry("[data, postdata] from [predata, postdata] backup ", DataPostdataArg, history.Predata|history.Postdata, MatchError("Cannot restore: [data, postdata] from backup containing: [predata, postdata]")),
			Entry("[data, postdata] from [data, postdata] backup ", DataPostdataArg, history.Data|history.Postdata, Not(HaveOccurred())),
			Entry("[data, postdata] from [predata, data, postdata] backup ", DataPostdataArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),

			// --section=predata --section=data --section=postdata
			Entry("[predata, data, postdata] from [predata] backup ", AllArg, history.Predata, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [predata]")),
			Entry("[predata, data, postdata] from [data] backup ", AllArg, history.Data, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [data]")),
			Entry("[predata, data, postdata] from [postdata] backup ", AllArg, history.Postdata, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [postdata]")),
			Entry("[predata, data, postdata] from [predata, data] backup ", AllArg, history.Predata|history.Data, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [predata, data]")),
			Entry("[predata, data, postdata] from [predata, postdata] backup ", AllArg, history.Predata|history.Postdata, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [predata, postdata]")),
			Entry("[predata, data, postdata] from [data, postdata] backup ", AllArg, history.Data|history.Postdata, MatchError("Cannot restore: [predata, data, postdata] from backup containing: [data, postdata]")),
			Entry("[predata, data, postdata] from [predata, data, postdata] backup ", AllArg, history.Predata|history.Data|history.Postdata, Not(HaveOccurred())),
		)
		It("Clears a section", func() {
			s.Clear(history.Predata)
			Expect(s.Is(history.Data | history.Postdata))
			s.Clear(history.Data)
			Expect(s.Is(history.Postdata))
			s.Clear(history.Postdata)
			Expect(s.Is(history.Empty))
		})
		It("Sets a section", func() {
			s.Set(history.Empty)
			Expect(s.Is(history.Empty))
			s.Set(history.Predata)
			Expect(s.Is(history.Predata))
			s.Set(history.Data)
			Expect(s.Is(history.Predata | history.Data))
			s.Set(history.Postdata)
			Expect(s.Is(history.Predata | history.Data | history.Postdata))
		})
	})
})

func defaultFlags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("section", pflag.ExitOnError)
	_ = flags.Bool(options.METADATA_ONLY, false, "")
	_ = flags.Bool(options.DATA_ONLY, false, "")
	_ = flags.StringSlice(options.SECTION, []string{}, "")
	_ = flags.Bool(options.CREATE_DB, false, "")
	_ = flags.Bool(options.WITH_GLOBALS, false, "")
	_ = flags.Bool(options.INCREMENTAL, false, "")
	_ = flags.Bool(options.LEAF_PARTITION_DATA, false, "")
	return flags
}
