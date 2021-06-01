package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type TesterYaml struct {
	List   []string            `yaml:"list"`
	Maps   map[string]uint32   `yaml:"maps"`
	Nested map[string][]string `yaml:"nested"`
}

func TestReadYaml(t *testing.T) {
	sample := TesterYaml{}
	err := LoadFromYaml("./tests/test.yaml", &sample)
	if err != nil {
		t.Errorf("Load failure: %s", err)
	}
	if sample.List == nil || len(sample.List) <= 0 {
		t.Error("load list fail")
	}
	if sample.Maps == nil || len(sample.Maps) <= 0 {
		t.Error("laod maps fail")
	}
	if sample.Nested == nil || len(sample.Nested) <= 0 {
		t.Error("load nested map fail")
	}
}

//
func TestReadNested(t *testing.T) {
	sample := TesterYaml{}
	LoadFromYaml("./tests/test.yaml", &sample)

	// assert nested
	if nestedMap, exists := sample.Nested["first"]; exists {
		// expect nestedMap length 2
		for i, s := range nestedMap {
			t.Log(s)
			if 2 < i {
				t.Fail()
			}
		}
	} else {
		t.Error("nested element not exists")
	}
}

func TestWriteYaml(t *testing.T) {
	sample := TesterYaml{}
	LoadFromYaml("./tests/test.yaml", &sample)
	ek := "하나"
	eins := []string{"둘", "셋", "넷"}
	sample.Nested[ek] = eins

	rspath := fmt.Sprintf("./tests/out.%s.yaml", time.Now().Format("060102_150405"))

	SaveToYaml(rspath, &sample)
	// book to delete the file
	defer os.Remove(rspath)
	//
	nexts := TesterYaml{}
	LoadFromYaml(rspath, &nexts)
	if len(eins) != len(nexts.Nested[ek]) {
		t.Error("saved length diff")
	}

	for i, v := range nexts.Nested[ek] {
		if v != eins[i] {
			t.Errorf("[%d] not equals exp: %s !=  act: %s", i, eins[i], v)
		}
	}

}

func TestConfigure(t *testing.T) {
	conf := GetConfigure("./cron.yaml")
	if conf.Connectors == nil || len(conf.Connectors) <= 0 {
		t.Error("no connector")
	}
	if len(conf.Successor) <= 0 {
		t.Error("no successor")
	}
	if conf.Targets == nil || len(conf.Targets) <= 0 {
		t.Error("no target")
	}
}
