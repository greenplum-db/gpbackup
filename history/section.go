package history

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/pflag"
)

//
// Sections is a bitmask that represents the different sections of a backup.
// The value is encoded as a uint32 and stored in both the backup configuration
// file and history database. During restore, the sections present in the backup
// configuration file are compared against the section flag arguments passed
// by the user to determine whether the request is valid, and which sections to restore.
//
// The sections are encoded as follows:
// Binary | Integer | Section
// ------ | ------- | -------
// 0000   | 0       | Empty
// 0001   | 1       | Predata
// 0010   | 2       | Data
// 0011   | 3       | (Predata | Data)
// 0100   | 4       | Postdata
// 0101   | 5       | (Predata | Postdata)
// 0110   | 6       | (Data | Postdata)
// 0111   | 7       | (Predata | Data | Postdata)
//
// Additional sections may be added in the future.
//
type Sections uint32

const (
	Predata Sections = 1 << iota
	Data
	Postdata
	Empty Sections = 0

	predataStr  = "predata"
	dataStr     = "data"
	postdataStr = "postdata"
)

func (s *Sections) Set(value Sections) {
	*s |= value
}

func (s *Sections) Clear(value Sections) {
	*s &= ^value
}

func (s *Sections) Contains(value Sections) bool {
	return *s&value == value
}

func (s *Sections) Is(value Sections) bool {
	return *s == value
}

func (s *Sections) ToString() string {
	sections := []string{}

	if s.Is(Empty) {
		return ""
	}

	if s.Contains(Predata) {
		sections = append(sections, predataStr)
	}
	if s.Contains(Data) {
		sections = append(sections, dataStr)
	}
	if s.Contains(Postdata) {
		sections = append(sections, postdataStr)
	}
	return strings.Join(sections, ", ")
}

func (s *Sections) FromString(sections ...string) error {
	for _, section := range sections {
		section = strings.ToLower(section)
		section = strings.ReplaceAll(section, " ", "")
		section = strings.ReplaceAll(section, "-", "")
		section = strings.ReplaceAll(section, "=", "")
		switch section {
		case predataStr:
			s.Set(Predata)
		case dataStr:
			s.Set(Data)
		case postdataStr:
			s.Set(Postdata)
		case "":
			return fmt.Errorf("No sections provided")
		default:
			return fmt.Errorf("Unrecognized section name: %s", section)
		}
	}
	return nil
}

func NewSections() *Sections {
	s := new(Sections)
	s.Set(Empty)
	return s
}

func (s *Sections) SetBackup(cmdFlags *pflag.FlagSet) error {
	err := s.parseFlags(cmdFlags)
	if err != nil {
		return err
	}

	if s.Is(Empty) {
		s.Set(Predata | Data | Postdata)
	}

	if !s.Contains(Data) {
		if cmdFlags.Changed(options.INCREMENTAL) {
			return fmt.Errorf("Cannot use --%s without section: data", options.INCREMENTAL)
		}
		if cmdFlags.Changed(options.LEAF_PARTITION_DATA) {
			return fmt.Errorf("Cannot use --%s without section: data", options.LEAF_PARTITION_DATA)
		}
	}

	return nil
}

func (s *Sections) SetRestore(cmdFlags *pflag.FlagSet, config *BackupConfig) error {

	if config == nil {
		return fmt.Errorf(("Empty backup config"))
	}

	configSections := parseConfigSections(config)

	err := s.parseFlags(cmdFlags)
	if err != nil {
		return err
	}

	if s.Is(Empty) {
	// 1. Restore sections and config sections are both empty. Restore all sections.
		if configSections.Is(Empty) {
			gplog.Verbose("Restoring all sections")
			s.Set(Predata | Data | Postdata)
			return nil
		} else {
	// 2. Restore sections are empty and config sections not empty. Restore sections
	//    from the backup config.
			s.Set(*configSections)
			gplog.Verbose("Restoring sections: [%s]", s.ToString())
			return nil
		}
	// 3. Restore sections not empty and missing predata. Validate exclusive flags.
	} else if !s.Contains(Predata) {
		if cmdFlags.Changed(options.CREATE_DB) {
			return fmt.Errorf("Cannot use --%s without section: predata", options.CREATE_DB)
		}
		if cmdFlags.Changed(options.WITH_GLOBALS) {
			return fmt.Errorf("Cannot use --%s without section: predata", options.WITH_GLOBALS)
		}
	}

	// 3. Restore sections are not empty and config sections are empty.
	//    This indicates a full backup taken prior to the sections feature.
	if configSections.Is(Empty) {
		gplog.Verbose("Restoring [%s] from backup without section information", s.ToString())
		return nil
	}

	// 4. Restore sections and config sections are not empty. Ensure that the sections
	//    requested are present in the backup config.
	if !config.Sections.Contains(*s) {
			return fmt.Errorf("Cannot restore: [%s] from backup containing: [%s]",
			s.ToString(),
			config.Sections.ToString())
	}
	
	return nil
}

func (s *Sections) parseFlags(cmdFlags *pflag.FlagSet) error {

	if cmdFlags.Changed(options.SECTION) {
		flags, err := cmdFlags.GetStringSlice(options.SECTION)
		if err != nil {
			return err
		} else if len(flags) == 0 {
			return fmt.Errorf("No section flags provided")
		}
		err = s.FromString(flags...)
		if err != nil {
			return err
		}
	} else if cmdFlags.Changed(options.METADATA_ONLY) {
		s.Set(Predata | Postdata)
	} else if cmdFlags.Changed(options.DATA_ONLY) {
		s.Set(Data)
	}

	return nil
}

func parseConfigSections(config *BackupConfig) *Sections {
	sections := NewSections()
	if !config.Sections.Is(Empty) {
		sections.Set(config.Sections)
	} else if config.MetadataOnly {
		sections.Set(Predata | Postdata)
	} else if config.DataOnly {
		sections.Set(Data)
	} else {
		sections.Set(Empty)
	}
	return sections
}
