package utils_test

import (
	"errors"
	"io"
	"os"

	"github.com/greenplum-db/gpbackup/testutils"
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
	Describe("CreateDirectoryOnMaster", func() {
		It("does nothing if the directory exists", func() {
			fakeInfo, _ := os.Stat("/tmp/log_dir")
			utils.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, nil }
			defer func() { utils.System.Stat = os.Stat }()
			utils.CreateDirectoryOnMaster("dirname")
		})
		It("panics if the directory does not exist", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) { return nil, errors.New("No such file or directory") }
			defer func() { utils.System.Stat = os.Stat }()
			defer testutils.ShouldPanicWithMessage("Cannot stat directory dirname: No such file or directory")
			utils.CreateDirectoryOnMaster("dirname")
		})
	})

	Describe("MustOpenFileForWriting", func() {
		It("creates or opens the file for writing", func() {
			utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return os.Stdout, nil }
			defer func() { utils.System.OpenFileWrite = utils.OpenFileWrite }()
			fileHandle := utils.MustOpenFileForWriting("filename")
			Expect(fileHandle).To(Equal(os.Stdout))
		})
		It("panics on error", func() {
			utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("Permission denied")
			}
			defer func() { utils.System.OpenFileWrite = utils.OpenFileWrite }()
			defer testutils.ShouldPanicWithMessage("Unable to create or open file for writing: Permission denied")
			utils.MustOpenFileForWriting("filename")
		})
	})
	Describe("MustOpenFileForReading", func() {
		It("creates or opens the file for reading", func() {
			utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) { return os.Stdin, nil }
			defer func() { utils.System.OpenFileRead = utils.OpenFileRead }()
			fileHandle := utils.MustOpenFileForReading("filename")
			Expect(fileHandle).To(Equal(os.Stdin))
		})
		It("panics on error", func() {
			utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) {
				return nil, errors.New("Permission denied")
			}
			defer func() { utils.System.OpenFileRead = utils.OpenFileRead }()
			defer testutils.ShouldPanicWithMessage("Unable to open file for reading: Permission denied")
			utils.MustOpenFileForReading("filename")
		})
	})
	Describe("FileExistsAndIsReadable", func() {
		AfterEach(func() {
			utils.System = utils.InitializeSystemFunctions()
		})
		It("returns true if the file both exists and is readable", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, nil
			}
			utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) {
				return &os.File{}, nil
			}
			check := utils.FileExistsAndIsReadable("filename")
			Expect(check).To(BeTrue())
		})
		It("returns false if the file does not exist", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, os.ErrNotExist
			}
			check := utils.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
		It("returns false if there is an error accessing the file", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, os.ErrPermission
			}
			check := utils.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
		It("returns false if there is an error opening the file", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, nil
			}
			utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) {
				return &os.File{}, &os.PathError{}
			}
			check := utils.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
	})
	Describe("ReadLinesFromFile", func() {
		fileContents := []byte(`public.foo
public."bar%baz"`)
		expectedContents := []string{`public.foo`, `public."bar%baz"`}

		It("reads a file containing multiple lines", func() {
			r, w, _ := os.Pipe()
			utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) { return r, nil }
			defer func() { utils.System.OpenFileRead = utils.OpenFileRead }()
			w.Write(fileContents)
			w.Close()
			contents := utils.ReadLinesFromFile("/tmp/table_file")
			Expect(contents).To(Equal(expectedContents))
		})
	})
	Describe("MustPrintf", func() {
		It("writes to a writable file", func() {
			utils.MustPrintf(buffer, "%s", "text")
			Expect(string(buffer.Contents())).To(Equal("text"))
		})
		It("panics on error", func() {
			defer testutils.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintf(os.Stdin, "text")
		})
	})
	Describe("MustPrintln", func() {
		It("writes to a writable file", func() {
			utils.MustPrintln(buffer, "text")
			Expect(string(buffer.Contents())).To(Equal("text\n"))
		})
		It("panics on error", func() {
			defer testutils.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintln(os.Stdin, "text")
		})
	})
	Describe("Close", func() {
		var file *utils.FileWithByteCount
		var wasCalled bool
		BeforeEach(func() {
			wasCalled = false
			utils.System.Chmod = func(name string, mode os.FileMode) error {
				wasCalled = true
				return nil
			}
			utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return &os.File{}, nil
			}
		})
		AfterEach(func() {
			utils.System.OpenFileWrite = utils.OpenFileWrite
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
			defer testutils.ShouldPanicWithMessage("invalid memory address or nil pointer dereference")
			file.MustPrintf("message")
		})
		It("closes the FileWithByteCount and makes it read-only if it has a filename", func() {
			file = utils.NewFileWithByteCountFromFile("testfile")
			file.Close()
			Expect(wasCalled).To(BeTrue())
			defer testutils.ShouldPanicWithMessage("invalid memory address or nil pointer dereference")
			file.MustPrintf("message")
		})
	})
	Describe("CreateBackupLockFile", func() {
		It("Does not panic if lock file does not exist for current timestamp", func() {
			utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, nil
			}
			utils.CreateBackupLockFile("20170101010101")
		})
		It("Panics if lock file exists for current timestamp", func() {
			utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("file does not exist")
			}
			defer testutils.ShouldPanicWithMessage("A backup with timestamp 20170101010101 is already in progress. Wait 1 second and try the backup again.")
			utils.CreateBackupLockFile("20170101010101")
		})
	})
})
