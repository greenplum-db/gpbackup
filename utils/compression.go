package utils

import "fmt"

var (
	pipeThroughProgram PipeThroughProgram
)

type PipeThroughProgram struct {
	Name          string
	OutputCommand string
	InputCommand  string
	Extension     string
}

func InitializePipeThroughParameters(compress bool, compressionLevel int) {
	if compress {
		pipeThroughProgram = PipeThroughProgram{Name: "gzip", OutputCommand: fmt.Sprintf("gzip -c -%d", compressionLevel), InputCommand: "gzip -d -c", Extension: ".gz"}
	} else {
		pipeThroughProgram = PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""}
	}
}

func GetPipeThroughProgram() PipeThroughProgram {
	return pipeThroughProgram
}

func SetPipeThroughProgram(compression PipeThroughProgram) {
	pipeThroughProgram = compression
}
