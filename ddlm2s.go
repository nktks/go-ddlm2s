package ddlm2s

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jinzhu/inflection"
	"github.com/k0kubun/pp"
	"github.com/knocknote/vitess-sqlparser/sqlparser"
)

type CreateStatements []*CreateStatement

func (all CreateStatements) findBy(tableName string) *CreateStatement {
	for _, c := range all {
		if fmt.Sprintf("%s", c.stmt.DDL.NewName.Name) == tableName {
			return c
		}
	}
	panic("cant find stmt by tablename")
}

type CreateStatement struct {
	stmt    *sqlparser.CreateTable
	indices []Index
}

func (c *CreateStatement) Print(debug bool) {
	if debug {
		fmt.Println("------------------------after-----------------------------")
		pp.Print(c.stmt)
	}
	tbuf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {})
	c.stmt.SpannerFormat(tbuf)
	fmt.Printf("%s;\n", string(tbuf.Buffer.String()))

	for _, index := range c.indices {
		fmt.Println(index.CreateDdl())
	}
}

func (c *CreateStatement) InterLeaveDepth(all CreateStatements) int {
	depth := 0
	parent := c.GetInterleaveParentStatement(all)
	if parent != nil {
		depth += 1
		depth += parent.InterLeaveDepth(all)
	}
	return depth
}

func (c *CreateStatement) GetInterleaveParentStatement(all CreateStatements) *CreateStatement {
	for _, constraint := range c.stmt.Constraints {
		if constraint.Type == sqlparser.ConstraintInterleave {
			return all.findBy(constraint.Name)
		}
	}
	return nil
}
func (c *CreateStatement) GetPrimaryKey() *sqlparser.Constraint {
	for _, constraint := range c.stmt.Constraints {
		if constraint.Type == sqlparser.ConstraintPrimaryKey {
			return constraint
		}
	}
	return nil
}

func (c *CreateStatement) GetColumns(colKeys []sqlparser.ColIdent) []*sqlparser.ColumnDef {
	var defs []*sqlparser.ColumnDef
	for _, key := range colKeys {
		strKey := fmt.Sprintf("%v", key)

		for _, c := range c.stmt.Columns {
			if c.Name == strKey {
				defs = append(defs, c)
				continue
			}
		}
	}
	return defs
}

func (c *CreateStatement) updatePrimaryKeyByInterleaveParent(all CreateStatements) {
	parent := c.GetInterleaveParentStatement(all)
	if parent == nil {
		return
	}
	var newConstraints []*sqlparser.Constraint
	for _, constraint := range c.stmt.Constraints {
		if constraint.Type == sqlparser.ConstraintPrimaryKey {
			constraint.Keys = append(parent.GetPrimaryKey().Keys, constraint.Keys...)
		}
		newConstraints = append(newConstraints, constraint)
	}
	c.stmt.Constraints = newConstraints
	if c.InterLeaveDepth(all) > 1 {
		pks := c.GetPrimaryKey().Keys
		lackedPks := pks[:len(pks)-2]
		lackedColumns := parent.GetColumns(lackedPks)
		c.stmt.Columns = append(lackedColumns, c.stmt.Columns...)
	}

}

func Convert(sqls string, debug bool) {
	var all CreateStatements
	for _, sql := range strings.Split(sqls, ";") {
		c := convert(sql, debug)
		if c != nil {
			all = append(all, c)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].InterLeaveDepth(all) < all[j].InterLeaveDepth(all) })
	var updatedAll CreateStatements
	for _, c := range all {
		c.updatePrimaryKeyByInterleaveParent(updatedAll)
		updatedAll = append(updatedAll, c)
		c.Print(debug)
	}
}
func convert(sql string, debug bool) *CreateStatement {
	sql = strings.Replace(sql, "\n", "", -1)
	if sql == "" {
		return nil
	}
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(err)
	}
	stmtC := stmt.(*sqlparser.CreateTable)
	if debug {
		fmt.Println("------------------------before-----------------------------")
		pp.Print(stmtC)
	}
	tableName := fmt.Sprintf("%s", stmtC.DDL.NewName.Name)
	stmtC.Columns = updateColumns(stmtC.Columns, tableName)
	constraints, indices := updateConstraints(stmtC.Constraints, tableName)
	stmtC.Constraints = constraints
	return &CreateStatement{
		stmt:    stmtC,
		indices: indices,
	}
}

// https://cloud.google.com/solutions/migrating-mysql-to-spanner?hl=ja#supported_data_types
func convertType(mysqlType string) string {
	if strings.HasPrefix(mysqlType, "int") ||
		strings.HasPrefix(mysqlType, "bigint") ||
		strings.HasPrefix(mysqlType, "mediumint") ||
		strings.HasPrefix(mysqlType, "smallint") ||
		strings.HasPrefix(mysqlType, "tinyint") {
		return "INT64"
	}
	if strings.HasPrefix(mysqlType, "bool") {
		return "BOOL"
	}
	if strings.HasPrefix(mysqlType, "float") || strings.HasPrefix(mysqlType, "double") {
		return "FLOAT64"
	}
	if strings.HasPrefix(mysqlType, "decimal") || strings.HasPrefix(mysqlType, "numeric") {
		panic("spanner dont support storing numeric data. please use float.")
	}
	if strings.HasPrefix(mysqlType, "bit") {
		return "BYTES"
	}
	if mysqlType == "date" {
		return "DATE"
	}
	if mysqlType == "datetime" || mysqlType == "timestamp" {
		return "TIMESTAMP"
	}
	if strings.HasPrefix(mysqlType, "char") {
		return buildSppnerTypeWithLength(mysqlType, "char", "STRING", 1)
	}
	if strings.HasPrefix(mysqlType, "varchar") {
		return buildSppnerTypeWithLength(mysqlType, "varchar", "STRING", 1)
	}
	if strings.HasPrefix(mysqlType, "binary") {
		return buildSppnerTypeWithLength(mysqlType, "binary", "BYTES", 1)
	}
	if strings.HasPrefix(mysqlType, "varbinary") {
		return buildSppnerTypeWithLength(mysqlType, "varbinary", "BYTES", 1)
	}
	if strings.HasPrefix(mysqlType, "blob") {
		return buildSppnerTypeWithLength(mysqlType, "blob", "BYTES", 65535)
	}
	if strings.HasPrefix(mysqlType, "tinyblob") {
		return buildSppnerTypeWithLength(mysqlType, "blob", "BYTES", 255)
	}
	if strings.HasPrefix(mysqlType, "text") {
		return buildSppnerTypeWithLength(mysqlType, "text", "STRING", 65535)
	}
	if strings.HasPrefix(mysqlType, "tinytext") {
		return buildSppnerTypeWithLength(mysqlType, "text", "STRING", 255)
	}
	if strings.HasPrefix(mysqlType, "enum") {
		// mysql does not have explicit enum element size limt.
		// https://dev.mysql.com/doc/refman/5.6/ja/limits-frm-file.html
		// this size is my temporary
		return "STRING(5)"
	}
	if strings.HasPrefix(mysqlType, "set") {
		// mysql does not have explicit set element size limt.
		// https://dev.mysql.com/doc/refman/5.6/ja/limits-frm-file.html
		// this size is my temporary
		return "ARRAY<STRING(5)>"
	}
	if strings.HasPrefix(mysqlType, "longblob") || strings.HasPrefix(mysqlType, "mediumblob") {
		panic("spanner dont support large blob.please use gcs and have uri column.")
	}
	if strings.HasPrefix(mysqlType, "longtext") || strings.HasPrefix(mysqlType, "mediumtext") {
		panic("spanner dont support large text.please use gcs and have uri column.")
	}
	if strings.HasPrefix(mysqlType, "json") {
		panic("spanner dont support large text.please use gcs and have uri column.")
	}
	panic("unexpected type.")
	return ""
}

func buildSppnerTypeWithLength(orgType, mysqlBaseType, spannerBaseType string, mysqlDefaultLength int) string {
	if strings.Contains(orgType, "(") {
		return strings.Replace(orgType, mysqlBaseType, spannerBaseType, 1)
	} else {
		return fmt.Sprintf("%s(%d)", spannerBaseType, mysqlDefaultLength)
	}
}

func updateColumns(columns []*sqlparser.ColumnDef, tableName string) []*sqlparser.ColumnDef {
	var newColumns []*sqlparser.ColumnDef
	for _, column := range columns {
		if column.Name == "id" {
			column.Name = fmt.Sprintf("%s_id", inflection.Singular(tableName))
		}
		var options []*sqlparser.ColumnOption
		for _, option := range column.Options {
			// TODO to index column attribute (ie. UNIQUE)
			if option.Type == sqlparser.ColumnOptionAutoIncrement {
				continue
			}
			if option.Type == sqlparser.ColumnOptionDefaultValue {
				continue
			}
			options = append(options, option)
		}
		column.Options = options
		column.Type = convertType(column.Type)
		newColumns = append(newColumns, column)
	}
	return newColumns
}

func updateConstraints(constraints []*sqlparser.Constraint, tableName string) ([]*sqlparser.Constraint, []Index) {
	var newConstraints []*sqlparser.Constraint
	var indices []Index
	// spanner table has only one InterLeave.
	// ddlm2s convert first fk to interleave clause.
	var interleavedFk *sqlparser.Constraint
	for _, constraint := range constraints {
		switch constraint.Type {
		case sqlparser.ConstraintPrimaryKey:
			keys := []sqlparser.ColIdent{}
			for _, key := range constraint.Keys {
				strKey := fmt.Sprintf("%v", key)
				if strKey == "id" {
					keys = append(keys, sqlparser.NewColIdent(fmt.Sprintf("%s_id", inflection.Singular(tableName))))
				} else {
					keys = append(keys, key)
				}
			}
			constraint.Keys = keys
		case sqlparser.ConstraintKey, sqlparser.ConstraintIndex, sqlparser.ConstraintUniq, sqlparser.ConstraintUniqKey, sqlparser.ConstraintUniqIndex:
			indices = append(indices, NewIndex(constraint, tableName))
			continue
		case sqlparser.ConstraintForeignKey:
			if interleavedFk != nil {
				continue
			}
			interleavedFk = constraint

			constraint = &sqlparser.Constraint{
				Type: sqlparser.ConstraintInterleave,
				Name: constraint.Reference.Name,
				Keys: []sqlparser.ColIdent{},
			}
		case sqlparser.ConstraintFulltext:
			panic("spanner dont support FULLTEXT")
		}

		newConstraints = append(newConstraints, constraint)
	}
	return newConstraints, indices
}
