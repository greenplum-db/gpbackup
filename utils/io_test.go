package utils_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/io tests", func() {
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
		It("does nothing if the FileWithByteCount's closer is nil", func() {
			file = utils.NewFileWithByteCount(buffer)
			file.Close()
			file.MustPrintf("message")
		})
		It("closes the FileWithByteCount and makes it read-only if it has a filename", func() {
			_ = os.Remove("testfile")
			defer os.Remove("testfile")
			file = utils.NewFileWithByteCountFromFile("testfile")
			file.Close()
			defer testhelper.ShouldPanicWithMessage("write testfile: file already closed: Unable to write to file")
			file.MustPrintf("message")
		})
	})
	Describe("CopyFile", func() {
		var sourceFilePath = "/tmp/test_file.txt"
		var destFilePath = "/tmp/dest_test_file.txt"

		BeforeEach(func() {
			_ = os.Remove(sourceFilePath)
			_ = os.Remove(destFilePath)
		})
		AfterEach(func() {
			_ = os.Remove(sourceFilePath)
			_ = os.Remove(destFilePath)
		})
		It("copies source file to dest file", func() {
			_ = ioutil.WriteFile(sourceFilePath, []byte{1, 2, 3, 4}, 0777)

			err := utils.CopyFile(sourceFilePath, destFilePath)

			Expect(err).ToNot(HaveOccurred())
			contents, _ := ioutil.ReadFile(destFilePath)
			Expect(contents).To(Equal([]byte{1, 2, 3, 4}))
		})
		It("returns an err when cannot read source file", func() {
			operating.System.Stat = func(f string) (os.FileInfo, error) {
				return nil, nil
			}

			err := utils.CopyFile(sourceFilePath, destFilePath)

			Expect(err).To(HaveOccurred())
		})
		It("returns an error when no source file exists", func() {
			err := utils.CopyFile(sourceFilePath, destFilePath)

			Expect(err).To(HaveOccurred())
		})
	})
})
