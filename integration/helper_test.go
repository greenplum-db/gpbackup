package integration

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	examplePluginExec          string
	examplePluginTestConfig    = "/tmp/test_example_plugin_config.yaml"
	examplePluginTestBackupDir = "/tmp/plugin_dest/20180101/20180101010101"
	examplePluginTestDataFile  = filepath.Join(examplePluginTestBackupDir, "test_data")
	examplePluginTestDir       = "/tmp/plugin_dest" // hardcoded in example plugin
	testDir                    = "/tmp/helper_test/20180101/20180101010101"
	tocFile                    = fmt.Sprintf("%s/test_toc.yaml", testDir)
	oidFile                    = fmt.Sprintf("%s/test_oids", testDir)
	pipeFile                   = fmt.Sprintf("%s/test_pipe", testDir)
	dataFileFullPath           = filepath.Join(testDir, "test_data")
	errorFile                  = fmt.Sprintf("%s_error", pipeFile)
)

const (
	defaultData  = "here is some data\n"
	expectedData = `here is some data
here is some data
here is some data
`
	expectedTOC = `dataentries:
  1:
    startbyte: 0
    endbyte: 18
  2:
    startbyte: 18
    endbyte: 36
  3:
    startbyte: 36
    endbyte: 54
`
)

func gpbackupHelper(helperPath string, args ...string) *exec.Cmd {
	args = append([]string{"--toc-file", tocFile, "--oid-file", oidFile, "--pipe-file", pipeFile, "--content", "1", "--single-data-file"}, args...)
	command := exec.Command(helperPath, args...)
	err := command.Start()
	Expect(err).ToNot(HaveOccurred())
	return command
}

func buildAndInstallBinaries() string {
	_ = os.Chdir("..")
	command := exec.Command("make", "build")
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	_ = os.Chdir("integration")
	binDir := fmt.Sprintf("%s/bin", operating.System.Getenv("GOPATH"))
	return fmt.Sprintf("%s/gpbackup_helper", binDir)
}

var _ = Describe("gpbackup_helper end to end integration tests", func() {
	// Setup example plugin based on current working directory
	err := os.RemoveAll(examplePluginTestDir)
	Expect(err).ToNot(HaveOccurred())
	err = os.MkdirAll(examplePluginTestDir, 0777)
	Expect(err).ToNot(HaveOccurred())
	currentDir, err := os.Getwd()
	Expect(err).ToNot(HaveOccurred())
	rootDir := path.Dir(currentDir)
	examplePluginExec = path.Join(rootDir, "plugins", "example_plugin.bash")
	examplePluginTestConfigContents := fmt.Sprintf(`executablepath: %s
options:
  password: unknown`, examplePluginExec)
	f, err := os.Create(examplePluginTestConfig)
	Expect(err).ToNot(HaveOccurred())
	_, err = f.WriteString(examplePluginTestConfigContents)
	Expect(err).ToNot(HaveOccurred())
	err = f.Close()
	Expect(err).ToNot(HaveOccurred())

	BeforeEach(func() {
		err := os.RemoveAll(testDir)
		Expect(err).ToNot(HaveOccurred())
		err = os.MkdirAll(testDir, 0777)
		Expect(err).ToNot(HaveOccurred())
		err = os.RemoveAll(examplePluginTestBackupDir)
		Expect(err).ToNot(HaveOccurred())
		err = os.MkdirAll(examplePluginTestBackupDir, 0777)
		Expect(err).ToNot(HaveOccurred())

		err = unix.Mkfifo(fmt.Sprintf("%s_%d", pipeFile, 1), 0777)
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
	})
	Context("backup tests", func() {
		BeforeEach(func() {
			f, _ := os.Create(oidFile)
			_, _ = f.WriteString("1\n2\n3\n")
		})
		It("runs backup gpbackup_helper without compression", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-level", "0", "--data-file", dataFileFullPath)
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifacts(false)
		})
		It("runs backup gpbackup_helper with data exceeding pipe buffer size", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-level", "0", "--data-file", dataFileFullPath)
			writeToPipes(strings.Repeat("a", int(math.Pow(2, 17))))
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
		})
		It("runs backup gpbackup_helper with gzip compression", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-type", "gzip", "--compression-level", "1", "--data-file", dataFileFullPath+".gz")
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifactsWithCompression("gzip", false)
		})
		It("runs backup gpbackup_helper with zstd compression", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-type", "zstd", "--compression-level", "1", "--data-file", dataFileFullPath+".zst")
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifactsWithCompression("zstd", false)
		})
		It("runs backup gpbackup_helper without compression with plugin", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-level", "0", "--data-file", dataFileFullPath, "--plugin-config", examplePluginTestConfig)
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifacts(true)
		})
		It("runs backup gpbackup_helper with gzip compression with plugin", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-type", "gzip", "--compression-level", "1", "--data-file", dataFileFullPath+".gz", "--plugin-config", examplePluginTestConfig)
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifactsWithCompression("gzip", true)
		})
		It("runs backup gpbackup_helper with zstd compression with plugin", func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-type", "zstd", "--compression-level", "1", "--data-file", dataFileFullPath+".zst", "--plugin-config", examplePluginTestConfig)
			writeToPipes(defaultData)
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertBackupArtifactsWithCompression("zstd", true)
		})
		It("Generates error file when backup agent interrupted", FlakeAttempts(5), func() {
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--backup-agent", "--compression-level", "0", "--data-file", dataFileFullPath)
			waitForPipeCreation()
			err := helperCmd.Process.Signal(unix.SIGINT)
			Expect(err).ToNot(HaveOccurred())
			err = helperCmd.Wait()
			Expect(err).To(HaveOccurred())
			assertErrorsHandled()
		})
	})
	Context("restore tests", func() {
		It("runs restore gpbackup_helper without compression", func() {
			setupRestoreFiles("", false)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath)
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("runs restore gpbackup_helper with gzip compression", func() {
			setupRestoreFiles("gzip", false)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".gz")
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("runs restore gpbackup_helper with zstd compression", func() {
			setupRestoreFiles("zstd", false)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".zst")
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("runs restore gpbackup_helper without compression with plugin", func() {
			setupRestoreFiles("", true)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath, "--plugin-config", examplePluginTestConfig)
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("runs restore gpbackup_helper with gzip compression with plugin", func() {
			setupRestoreFiles("gzip", true)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".gz", "--plugin-config", examplePluginTestConfig)
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("runs restore gpbackup_helper with zstd compression with plugin", func() {
			setupRestoreFiles("zstd", true)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".zst", "--plugin-config", examplePluginTestConfig)
			for _, i := range []int{1, 3} {
				contents, _ := ioutil.ReadFile(fmt.Sprintf("%s_%d", pipeFile, i))
				Expect(string(contents)).To(Equal("here is some data\n"))
			}
			err := helperCmd.Wait()
			printHelperLogOnError(err)
			Expect(err).ToNot(HaveOccurred())
			assertNoErrors()
		})
		It("Generates error file when restore agent interrupted", func() {
			setupRestoreFiles("gzip", false)
			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".gz", "--single-data-file")
			waitForPipeCreation()
			err := helperCmd.Process.Signal(unix.SIGINT)
			Expect(err).ToNot(HaveOccurred())
			err = helperCmd.Wait()
			Expect(err).To(HaveOccurred())
			assertErrorsHandled()
		})
		It("Continues restore process when encountering an error with flag --on-error-continue", func() {
			// Write data file
			dataFile := dataFileFullPath
			f, _ := os.Create(dataFile + ".gz")
			gzipf := gzip.NewWriter(f)
			// Named pipes can buffer, so we need to write more than the buffer size to trigger flush error
			customData := "here is some data\n"
			dataLength := 128*1024 + 1
			customData += strings.Repeat("a", dataLength)
			customData += "here is some data\n"

			_, _ = gzipf.Write([]byte(customData))
			_ = gzipf.Close()

			// Write oid file
			fOid, _ := os.Create(oidFile)
			_, _ = fOid.WriteString("1\n2\n3\n")
			defer func() {
				_ = os.Remove(oidFile)
			}()

			// Write custom TOC
			customTOC := fmt.Sprintf(`dataentries:
  1:
    startbyte: 0
    endbyte: 18
  2:
    startbyte: 18
    endbyte: %[1]d
  3:
    startbyte: %[1]d
    endbyte: %d
`, dataLength+18, dataLength+18+18)
			fToc, _ := os.Create(tocFile)
			_, _ = fToc.WriteString(customTOC)
			defer func() {
				_ = os.Remove(tocFile)
			}()

			helperCmd := gpbackupHelper(gpbackupHelperPath, "--restore-agent", "--data-file", dataFileFullPath+".gz", "--on-error-continue")

			for k, v := range []int{1, 2, 3} {
				currentPipe := fmt.Sprintf("%s_%d", pipeFile, v)

				if k == 1 {
					// Do not read from the pipe to cause data load error on the helper by interrupting the write.
					file, errOpen := os.Open(currentPipe)
					Expect(errOpen).ToNot(HaveOccurred())
					errClose := file.Close()
					Expect(errClose).ToNot(HaveOccurred())
				} else {
					contents, err := ioutil.ReadFile(currentPipe)
					Expect(err).ToNot(HaveOccurred())
					Expect(string(contents)).To(Equal("here is some data\n"))
				}
			}

			// Block here until gpbackup_helper finishes (cleaning up pipes)
			_ = helperCmd.Wait()
			for _, i := range []int{1, 2, 3} {
				currentPipe := fmt.Sprintf("%s_%d", pipeFile, i)
				Expect(currentPipe).ToNot(BeAnExistingFile())
			}

			// Check that an error file was created
			Expect(errorFile).To(BeAnExistingFile())
		})
	})
})

func setupRestoreFiles(compressionType string, withPlugin bool) {
	dataFile := dataFileFullPath
	if withPlugin {
		dataFile = examplePluginTestDataFile
	}

	f, _ := os.Create(oidFile)
	_, _ = f.WriteString("1\n3\n")

	if compressionType == "gzip" {
		f, _ := os.Create(dataFile + ".gz")
		defer f.Close()
		gzipf := gzip.NewWriter(f)
		defer gzipf.Close()
		_, _ = gzipf.Write([]byte(expectedData))
	} else if compressionType == "zstd" {
		f, _ := os.Create(dataFile + ".zst")
		defer f.Close()
		zstdf, _ := zstd.NewWriter(f)
		defer zstdf.Close()
		_, _ = zstdf.Write([]byte(expectedData))
	} else {
		f, _ := os.Create(dataFile)
		_, _ = f.WriteString(expectedData)
	}

	f, _ = os.Create(tocFile)
	_, _ = f.WriteString(expectedTOC)
}

func assertNoErrors() {
	Expect(errorFile).To(Not(BeARegularFile()))
	pipes, err := filepath.Glob(pipeFile + "_[1-9]*")
	Expect(err).ToNot(HaveOccurred())
	Expect(pipes).To(BeEmpty())
}

func assertErrorsHandled() {
	Expect(errorFile).To(BeARegularFile())
	pipes, err := filepath.Glob(pipeFile + "_[1-9]*")
	Expect(err).ToNot(HaveOccurred())
	Expect(pipes).To(BeEmpty())
}

func assertBackupArtifacts(withPlugin bool) {
	var contents []byte
	var err error
	dataFile := dataFileFullPath
	if withPlugin {
		dataFile = examplePluginTestDataFile
	}
	contents, err = ioutil.ReadFile(dataFile)
	Expect(err).ToNot(HaveOccurred())
	Expect(string(contents)).To(Equal(expectedData))

	contents, err = ioutil.ReadFile(tocFile)
	Expect(err).ToNot(HaveOccurred())
	Expect(string(contents)).To(Equal(expectedTOC))
	assertNoErrors()
}

func assertBackupArtifactsWithCompression(compressionType string, withPlugin bool) {
	var contents []byte
	var err error

	dataFile := dataFileFullPath
	if withPlugin {
		dataFile = examplePluginTestDataFile
	}

	if compressionType == "gzip" {
		contents, err = ioutil.ReadFile(dataFile + ".gz")
	} else if compressionType == "zstd" {
		contents, err = ioutil.ReadFile(dataFile + ".zst")
	} else {
		Fail("unknown compression type " + compressionType)
	}
	Expect(err).ToNot(HaveOccurred())

	if compressionType == "gzip" {
		r, _ := gzip.NewReader(bytes.NewReader(contents))
		contents, _ = ioutil.ReadAll(r)
	} else if compressionType == "zstd" {
		r, _ := zstd.NewReader(bytes.NewReader(contents))
		contents, _ = ioutil.ReadAll(r)
	} else {
		Fail("unknown compression type " + compressionType)
	}
	Expect(string(contents)).To(Equal(expectedData))

	contents, err = ioutil.ReadFile(tocFile)
	Expect(err).ToNot(HaveOccurred())
	Expect(string(contents)).To(Equal(expectedTOC))

	assertNoErrors()
}

func printHelperLogOnError(helperErr error) {
	if helperErr != nil {
		homeDir := os.Getenv("HOME")
		helperFiles, _ := filepath.Glob(filepath.Join(homeDir, "gpAdminLogs/gpbackup_helper_*"))
		command := exec.Command("tail", "-n 20", helperFiles[len(helperFiles)-1])
		output, _ := command.CombinedOutput()
		fmt.Println(string(output))
	}
}

func writeToPipes(data string) {
	for i := 1; i <= 3; i++ {
		currentPipe := fmt.Sprintf("%s_%d", pipeFile, i)
		_, err := os.Stat(currentPipe)
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
		f, _ := os.Create("/tmp/tmpdata.txt")
		_, _ = f.WriteString(data)
		output, err := exec.Command("bash", "-c", fmt.Sprintf("cat %s > %s", "/tmp/tmpdata.txt", currentPipe)).CombinedOutput()
		_ = f.Close()
		_ = os.Remove("/tmp/tmpdata.txt")
		if err != nil {
			fmt.Printf("%s", output)
			Fail(fmt.Sprintf("%v", err))
		}
	}
}

func waitForPipeCreation() {
	// wait up to 5 seconds for two pipe files to have been created
	tries := 0
	for tries < 1000 {
		pipes, err := filepath.Glob(pipeFile + "_[1-9]*")
		Expect(err).ToNot(HaveOccurred())
		if len(pipes) > 1 {
			return
		}

		tries += 1
		time.Sleep(5 * time.Millisecond)
	}
}
