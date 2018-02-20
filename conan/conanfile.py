from conans import ConanFile, tools
from io import StringIO
import os


class GpbackupConan(ConanFile):
    name = "gpbackup"
    license = "Apache License v2.0"
    url = "https://github.com/greenplum-db/gpbackup"
    description = "Greenplum DB backup and restore utilities"
    settings = "os", "compiler", "build_type", "arch"

    def source(self):
        self.run("git clone https://github.com/greenplum-db/gpbackup.git src/github.com/greenplum-db/gpbackup")
        with tools.chdir('src/github.com/greenplum-db/gpbackup'):
            ver = StringIO()
            self.run("git describe --tags", output=ver)
            self.run("git checkout " + ver.getvalue())

    def build(self):
        os.environ["PATH"] = os.environ["PATH"] + ":" + os.path.join(os.getcwd(), "bin")
        with tools.environment_append({'GOPATH': os.getcwd()}):
            with tools.chdir('src/github.com/greenplum-db/gpbackup'):
                self.run('dep ensure && make build_linux')

    def package(self):
        self.copy("gpbackup", dst="bin", src="bin")
        self.copy("gprestore", dst="bin", src="bin")
        self.copy("gpbackup_helper", dst="bin", src="bin")
