from conans import ConanFile, tools
import os


class GpbackupConan(ConanFile):
    name = "gpbackup"
    version = "0.2"
    license = "<Put the package license here>"
    url = "https://github.com/greenplum-db/gpbackup"
    description = "Greenplum DB backup and restor utilities"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources = "src/*", "src/github.com/greenplum-db/gpbackup/.git/*"

    def build(self):
        os.environ["PATH"] = os.environ["PATH"] + ":" + os.path.join(os.getcwd(), "bin")
        with tools.environment_append({'GOPATH': os.getcwd()}):
            with tools.chdir('src/github.com/greenplum-db/gpbackup'):
                self.run('make build')

    def package(self):
        self.copy("gpbackup", dst="bin", src="bin")
        self.copy("gprestore", dst="bin", src="bin")
