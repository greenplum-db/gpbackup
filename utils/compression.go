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

func InitializePipeThroughParameters(compress bool, compressionType string, compressionLevel int) {
	if !compress {
		pipeThroughProgram = PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""}
		return
	}

	// backward compatibility for inputs without compressionType
	if compressionType == "" {
		compressionType = "gzip"
	}

	if compressionType == "gzip" {
		pipeThroughProgram = PipeThroughProgram{Name: "gzip", OutputCommand: fmt.Sprintf("gzip -c -%d", compressionLevel), InputCommand: "gzip -d -c", Extension: ".gz"}
		return
	}
}

func GetPipeThroughProgram() PipeThroughProgram {
	return pipeThroughProgram
}

func SetPipeThroughProgram(compression PipeThroughProgram) {
	pipeThroughProgram = compression
}
