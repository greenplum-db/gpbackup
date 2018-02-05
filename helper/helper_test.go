package helper_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/helper"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("helper/helper", func() {
	var stdinRead, stdinWrite *os.File
	var tocFileRead *os.File
	BeforeEach(func() {
		stdinRead, stdinWrite, _ = os.Pipe()
		tocFileRead, _, _ = os.Pipe()
		operating.System.Stdin = stdinRead
		stdout = gbytes.NewBuffer()
		operating.System.Stdout = stdout
	})
	AfterEach(func() {
		operating.System.OpenFileRead = operating.OpenFileRead
		operating.System.Stat = os.Stat
		operating.System.Stdin = os.Stdin
		operating.System.Stdout = os.Stdout
		operating.System.ReadFile = ioutil.ReadFile
	})
	Describe("ReadAndCountBytes", func() {
		It("Returns correct number of bytes read", func() {
			fmt.Fprintln(stdinWrite, "some text")
			stdinWrite.Close()
			bytesRead := helper.ReadAndCountBytes()
			Expect(bytesRead).To(Equal(uint64(10)))
			Expect(stdout).To(gbytes.Say("some text\n"))
		})
		It("Returns 0 if no bytes read", func() {
			stdinWrite.Close()
			bytesRead := helper.ReadAndCountBytes()
			Expect(bytesRead).To(Equal(uint64(0)))
			Expect(stdout).To(gbytes.Say(""))
		})
		Describe("ReadOrCreateTOC", func() {
			It("returns contents of TOC when a TOC file exists", func() {
				helper.SetFilename("filename")
				operating.System.Stat = func(name string) (os.FileInfo, error) {
					return nil, nil
				}
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return tocFileRead, nil }
				operating.System.ReadFile = func(filename string) ([]byte, error) {
					return []byte(`lastbyteread: 15
dataentries:
  1:
    startbyte: 0
    endbyte: 5
  2:
    startbyte: 5
    endbyte: 10
  3:
    startbyte: 10
    endbyte: 15`), nil
				}
				expectedDataEntries := map[uint]utils.SegmentDataEntry{
					1: {StartByte: 0, EndByte: 5},
					2: {StartByte: 5, EndByte: 10},
					3: {StartByte: 10, EndByte: 15},
				}
				toc, lastRead := helper.ReadOrCreateTOC()
				Expect(lastRead).To(Equal(uint64(15)))
				Expect((*toc).DataEntries).To(Equal(expectedDataEntries))
			})
			It("returns a new TOC when no TOC file exists", func() {
				helper.SetFilename("filename")
				operating.System.Stat = func(name string) (os.FileInfo, error) {
					return nil, os.ErrNotExist
				}
				toc, lastRead := helper.ReadOrCreateTOC()
				Expect(lastRead).To(Equal(uint64(0)))
				Expect((*toc).DataEntries).To(Not(BeNil()))
			})
		})
	})
})
