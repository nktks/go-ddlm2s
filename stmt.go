package ddlm2s

import (
	"fmt"

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
