package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

const (
	ConfigPath     = "./cron.yaml"
	KEY_CNX_SOURCE = "legacy"
	KEY_CNX_TARGET = "replica"
)

/** Configure settings **/
var ServiceConfig *Settings

// Settings - yaml settings (cron.yaml)
type Settings struct {
	Connectors map[string]ConnectionSetting      `yaml:"connectors"` // Connectors determine database connector config
	Schedule   string                            `yaml:"schedule"`   // Crontab Schedule
	Successor  string                            `yaml:"successor"`  // (yaml) file that contains per-table latest synced row records
	Targets    map[string][]TableTransferSetting `yaml:"targets"`    // Schema(key) per transfer setups(per-table)
}

// ConnectionSetting - Database Connector
type ConnectionSetting struct {
	Driver string `yaml:"driver"`
	DSN    string `yaml:"dsn"`
}

// TableTransferSetting - Target transfer table
type TableTransferSetting struct {
	Name  string `yaml:"table"`
	Index string `yaml:"index"`
}

// LoadFromYaml - load any contents from yaml file.
// @param path - target file path,
// @param ptr - pointer to the result record
func LoadFromYaml(path string, ptr interface{}) error {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	// and the contents
	err = yaml.Unmarshal(contents, ptr)
	return err
}

// SaveToYaml - write yaml file
func SaveToYaml(path string, ptr interface{}) error {
	contents, err := yaml.Marshal(ptr)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path, contents, 0777)
	return err
}

func GetConfigure(path string) *Settings {
	if ServiceConfig == nil {
		// load yaml file
		ServiceConfig = &Settings{}
		err := LoadFromYaml(path, ServiceConfig)
		errorCheck(err, -2, "failure on load settings")
	}
	return ServiceConfig
}

/** Successor read/write **/
var SuccessConfig SuccessorSetting

//
type SuccessorSetting map[string]map[string]interface{}

func GetSuccessor(path string) SuccessorSetting {
	if SuccessConfig == nil {
		SuccessConfig = SuccessorSetting{}
		err := LoadFromYaml(path, SuccessConfig)
		errorCheck(err, -2, "failure on load success records")
	}
	return SuccessConfig
}
