package main

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/nakatamixi/go-ddlm2s"
)

func main() {
	var (
		file             string
		debug            bool
		enableInterleave bool
	)
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.StringVar(&file, "f", "", "sql file path")
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
