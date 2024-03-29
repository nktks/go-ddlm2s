module github.com/nakatamixi/go-ddlm2s

go 1.13

require (
	github.com/jinzhu/inflection v1.0.0
	github.com/juju/errors v0.0.0-20190930114154-d42613fe1ab9 // indirect
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/k0kubun/pp v3.0.1+incompatible
	github.com/knocknote/vitess-sqlparser v0.0.0-20190712090058-385243f72d33
	github.com/mattn/go-colorable v0.1.4 // indirect
	golang.org/x/text v0.3.8 // indirect
	gopkg.in/yaml.v2 v2.2.4
)

replace github.com/knocknote/vitess-sqlparser => github.com/nakatamixi/vitess-sqlparser v0.0.0-20191030035102-acd30bb46a50
