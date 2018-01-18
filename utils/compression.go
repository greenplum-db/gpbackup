package utils

import "fmt"

var (
	usingCompression   = true
	compressionProgram Compression
)

type Compression struct {
	Name              string
	CompressCommand   string
	DecompressCommand string
	Extension         string
}

func InitializeCompressionParameters(compress bool, compressionLevel int) {
	usingCompression = compress
	compressCommand := ""
	if compressionLevel == 0 {
		compressCommand = "gzip -c -1"
	} else {
		compressCommand = fmt.Sprintf("gzip -c -%d", compressionLevel)
	}
	compressionProgram = Compression{Name: "gzip", CompressCommand: compressCommand, DecompressCommand: "gzip -d -c", Extension: ".gz"}
}

func GetCompressionParameters() (bool, Compression) {
	return usingCompression, compressionProgram
}

func SetCompressionParameters(compress bool, compression Compression) {
	usingCompression = compress
	compressionProgram = compression
}
