package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/greenplum-db/gpbackup/testutils"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/backup"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpexpand", func() {
	BeforeEach(func() {
		testutils.SkipIfBefore6(backupConn)
	})

	It("should stop gpbackup from running when gpexpand is already running", func() {
		masterDataDir, err := dbconn.SelectString(backupConn, backup.MasterDataDirQuery)
		if err != nil {
			Fail("cannot get master data dir from db")
		}

		// SIMULATE that gpexpand is running by creating its status file
		path := filepath.Join(masterDataDir, backup.GpexpandStatusFilename)
		oidFp := iohelper.MustOpenFileForWriting(path)
		err = oidFp.Close()
		Expect(err).ToNot(HaveOccurred())

		defer func() {
			err = os.Remove(path)
			if err != nil {
				Fail(fmt.Sprintf("IMPORTANT: failed to delete gpexpand.status file, which will prevent any future run of gpbackup! Manually delete file at: %s\n", path))
			}
		}()

		cmd := exec.Command("gpbackup", "--dbname", "postgres")
		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("-[CRITICAL]:-Greenplum expansion currently in process, please re-run gpbackup when the expansion has completed"))
	})
})
