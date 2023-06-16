package utils_test

import (
	"path/filepath"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/vfs"
	"github.com/blang/vfs/memfs"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpexpand_sensor", func() {
	const sampleCoordinatorDataDir = "/my_fake_database/demoDataDir-1"
	var (
		memoryfs       vfs.Filesystem
		mddPathRow     *sqlmock.Rows
		tableExistsRow *sqlmock.Rows
	)

	BeforeEach(func() {
		memoryfs = memfs.Create()
		mddPathRow = sqlmock.NewRows([]string{"datadir"}).AddRow(sampleCoordinatorDataDir)
		tableExistsRow = sqlmock.NewRows([]string{"relname"}).AddRow("some table name")
		connectionPool.DBName = "postgres"

		if connectionPool.Version.Before("6") {
			Skip("gpexpand sensor only runs against GPDB 6+")
		}
	})
	Context("IsGpexpandRunning", func() {
		Describe("happy path", func() {
			It("senses when gpexpand is in phase 1, as determined by existence of a file 'gpexpand.status' in coordinator data directory", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				Expect(vfs.MkdirAll(memoryfs, sampleCoordinatorDataDir, 0755)).To(Succeed())
				path := filepath.Join(sampleCoordinatorDataDir, utils.GpexpandStatusFilename)
				Expect(vfs.WriteFile(memoryfs, path, []byte{0}, 0400)).To(Succeed())
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeTrue())
			})
			It("senses gpexpand is in phase 2, as determined by database query to postgres database for gpexpand's temporary table", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)

				mock.ExpectQuery(regexp.QuoteMeta(utils.GpexpandStatusTableExistsQuery)).WillReturnRows(tableExistsRow)
				hasGpexpandPhase2StatusRow := sqlmock.NewRows([]string{"status"}).AddRow("some gpexpand status that is not finished")
				mock.ExpectQuery(utils.GpexpandTemporaryTableStatusQuery).WillReturnRows(hasGpexpandPhase2StatusRow)
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeTrue())
			})
			It("senses when all indications are that gpexpand status does not exist", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				tableDoesNotExistsRow := sqlmock.NewRows([]string{"relname"}).AddRow("")

				mock.ExpectQuery(regexp.QuoteMeta(utils.GpexpandStatusTableExistsQuery)).WillReturnRows(tableDoesNotExistsRow)
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeFalse())
			})
			It("senses when gpexpand status indicates stoppage", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				mock.ExpectQuery(regexp.QuoteMeta(utils.GpexpandStatusTableExistsQuery)).WillReturnRows(tableExistsRow)
				finishedGpexpandPhase2StatusRow := sqlmock.NewRows([]string{"status"}).AddRow("EXPANSION STOPPED")
				mock.ExpectQuery(utils.GpexpandTemporaryTableStatusQuery).WillReturnRows(finishedGpexpandPhase2StatusRow)
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeFalse())
			})
			It("senses when gpexpand status indicates completion", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				mock.ExpectQuery(regexp.QuoteMeta(utils.GpexpandStatusTableExistsQuery)).WillReturnRows(tableExistsRow)
				finishedGpexpandPhase2StatusRow := sqlmock.NewRows([]string{"status"}).AddRow("EXPANSION COMPLETE")
				mock.ExpectQuery(utils.GpexpandTemporaryTableStatusQuery).WillReturnRows(finishedGpexpandPhase2StatusRow)
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeFalse())
			})
			It("senses when gpexpand status indicates completion", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				mock.ExpectQuery(regexp.QuoteMeta(utils.GpexpandStatusTableExistsQuery)).WillReturnRows(tableExistsRow)
				finishedGpexpandPhase2StatusRow := sqlmock.NewRows([]string{"status"}).AddRow("SETUP DONE")
				mock.ExpectQuery(utils.GpexpandTemporaryTableStatusQuery).WillReturnRows(finishedGpexpandPhase2StatusRow)
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				result, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(BeFalse())
			})
		})
		Describe("sad paths", func() {
			It("returns an error when MDD query fails", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnError(errors.New("query error"))
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("query error"))
			})
			It("returns an error when Stat for file fails for a reason besides 'does not exist'", func() {
				mock.ExpectQuery(utils.CoordinatorDataDirQuery).WillReturnRows(mddPathRow)
				gpexpandSensor := utils.NewGpexpandSensor(vfs.Dummy(errors.New("fs error")), connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("fs error"))
			})
			It("returns an error when supplied with a connection to a database != postgres", func() {
				connectionPool.DBName = "notThePostgresDatabase"
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("gpexpand sensor requires a connection to the postgres database"))
			})
			It("returns an error when supplied with Greenplum version < 6", func() {
				testhelper.SetDBVersion(connectionPool, "5.3.0")
				gpexpandSensor := utils.NewGpexpandSensor(memoryfs, connectionPool)

				_, err := gpexpandSensor.IsGpexpandRunning()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("gpexpand sensor requires a connection to Greenplum version >= 6"))
			})
		})
	})
})
