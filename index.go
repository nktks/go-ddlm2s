package ddlm2s

import (
	"fmt"
	"strings"

	"github.com/knocknote/vitess-sqlparser/sqlparser"
)

type Index struct {
	Name      string
	TableName string
	Keys      []string
	Unique    bool
}

func NewIndex(constraints *sqlparser.Constraint, tableName string) Index {
	keys := []string{}
	for _, key := range constraints.Keys {
		keys = append(keys, fmt.Sprintf("%v", key))
	}
	return Index{
		Name:      name(keys, tableName),
		TableName: tableName,
		Keys:      keys,
		Unique:    constraints.Type == sqlparser.ConstraintUniq || constraints.Type == sqlparser.ConstraintUniqKey || constraints.Type == sqlparser.ConstraintUniqIndex,
	}
}

func name(keys []string, tableName string) string {
	return fmt.Sprintf("idx_%s_%s", tableName, strings.Join(keys, "_"))
}

// https://cloud.google.com/spanner/docs/data-definition-language?hl=ja#index_statements
func (index Index) CreateDdl() string {
	uniqOption := ""
	if index.Unique {
		uniqOption = "UNIQUE"
	}
	indexName := index.Name
	if indexName == "" {
		indexName = fmt.Sprintf("%s_%s", index.TableName, strings.Join(index.Keys, "_"))
	}
	return fmt.Sprintf("CREATE %s INDEX %s ON %s (%s);", uniqOption, index.Name, index.TableName, strings.Join(index.Keys, ", "))
}
