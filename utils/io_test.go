package utils_test

import (
	"errors"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/io tests", func() {
	Describe("SliceToQuotedString", func() {
		It("quotes and joins a slice of strings into a single string", func() {
			inputStrings := []string{"string1", "string2", "string3"}
			expectedString := "'string1','string2','string3'"
			resultString := utils.SliceToQuotedString(inputStrings)
			Expect(resultString).To(Equal(expectedString))
		})
		It("returns an empty string when given an empty slice", func() {
			inputStrings := []string{}
			resultString := utils.SliceToQuotedString(inputStrings)
			Expect(resultString).To(Equal(""))
		})
	})
	Describe("MustPrintf", func() {
		It("writes to a writable file", func() {
			utils.MustPrintf(buffer, "%s", "text")
			Expect(string(buffer.Contents())).To(Equal("text"))
		})
		It("panics on error", func() {
			defer testhelper.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintf(os.Stdin, "text")
		})
	})
	Describe("MustPrintln", func() {
		It("writes to a writable file", func() {
			utils.MustPrintln(buffer, "text")
			Expect(string(buffer.Contents())).To(Equal("text\n"))
		})
		It("panics on error", func() {
			defer testhelper.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintln(os.Stdin, "text")
		})
	})
	Describe("Close", func() {
		var file *utils.FileWithByteCount
		var wasCalled bool
		BeforeEach(func() {
			wasCalled = false
			operating.System.Chmod = func(name string, mode os.FileMode) error {
				wasCalled = true
				return nil
			}
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return &os.File{}, nil
			}
		})
		AfterEach(func() {
			operating.System.OpenFileWrite = operating.OpenFileWrite
		})
		It("does nothing if the FileWithByteCount's closer is nil", func() {
			file = utils.NewFileWithByteCount(buffer)
			file.Close()
			file.MustPrintf("message")
		})
		It("closes the FileWithByteCount if it has no filename", func() {
			file = utils.NewFileWithByteCountFromFile("")
			file.Close()
			Expect(wasCalled).To(BeFalse())
			defer testhelper.ShouldPanicWithMessage("invalid memory address or nil pointer dereference")
			file.MustPrintf("message")
		})
		It("closes the FileWithByteCount and makes it read-only if it has a filename", func() {
			file = utils.NewFileWithByteCountFromFile("testfile")
			file.Close()
			Expect(wasCalled).To(BeTrue())
			defer testhelper.ShouldPanicWithMessage("invalid memory address or nil pointer dereference")
			file.MustPrintf("message")
		})
	})
	Describe("CreateBackupLockFile", func() {
		It("Does not panic if lock file does not exist for current timestamp", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, nil
			}
			utils.CreateBackupLockFile("20170101010101")
		})
		It("Panics if lock file exists for current timestamp", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("file does not exist")
			}
			defer testhelper.ShouldPanicWithMessage("A backup with timestamp 20170101010101 is already in progress. Wait 1 second and try the backup again.")
			utils.CreateBackupLockFile("20170101010101")
		})
	})
})
