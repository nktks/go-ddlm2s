package main

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/jinzhu/inflection"
	"github.com/nakatamixi/go-ddlm2s"
	"gopkg.in/yaml.v2"
)

type InflectionRule struct {
	UnCountable []string       `yaml:"uncountable"`
	Irregular   []IregularRule `yaml:"irregular"`
	Plulal      []FromTo       `yaml:"plulal"`
	Singular    []FromTo       `yaml:"singular"`
}

type IregularRule struct {
	Singuler string `yaml:"singular"`
	Plulal   string `yaml:"plulal"`
}

type FromTo struct {
	From string `yaml:"from"`
	To   string `yaml:"to"`
}

func main() {
	var (
		file             string
		ruleFile         string
		debug            bool
		enableInterleave bool
	)
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.StringVar(&file, "f", "", "sql file path")
	flags.StringVar(&ruleFile, "r", "", "inflection rule file path")
	flags.BoolVar(&debug, "d", false, "debug print")
	flags.BoolVar(&enableInterleave, "interleave", true, "convert fk to interleave")
	if err := flags.Parse(os.Args[1:]); err != nil {
		flags.Usage()
		return
	}
	if file == "" {
		flags.Usage()
		return
	}
	body, err := read(file)
	if err != nil {
		panic(err)
	}
	if ruleFile != "" {
		inflectionRule, err := readRule(ruleFile)
		if err != nil {
			panic(err)
		}
		if inflectionRule != nil {
			if inflectionRule.UnCountable != nil {
				for _, unc := range inflectionRule.UnCountable {
					inflection.AddUncountable(unc)
				}
			}
			if inflectionRule.Irregular != nil {
				for _, irr := range inflectionRule.Irregular {
					inflection.AddIrregular(irr.Singuler, irr.Plulal)
				}
			}
			if inflectionRule.Plulal != nil {
				for _, pl := range inflectionRule.Plulal {
					inflection.AddPlural(pl.From, pl.To)
				}
			}
			if inflectionRule.Singular != nil {
				for _, si := range inflectionRule.Singular {
					inflection.AddSingular(si.From, si.To)
				}
			}
		}
	}
	ddlm2s.Convert(body, debug, enableInterleave)
}

func read(file string) (string, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	body := string(data)
	return body, nil

}
func readRule(ruleFile string) (*InflectionRule, error) {
	data, err := ioutil.ReadFile(ruleFile)
	if err != nil {
		return nil, err
	}
	inflectionRule := InflectionRule{}
	err = yaml.Unmarshal(data, &inflectionRule)
	if err != nil {
		return nil, err
	}
	return &inflectionRule, nil

}
