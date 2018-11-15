package reviewer

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
)

const (
	DELETE_FROM       = iota
	DELETE_TABLE_LIST
	DELETE_WHERE
)

type DeleteVisitor struct {
	Level int
	HasWhereClause bool
	CurrentBlock int
}

func (this *DeleteVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	// fmt.Printf("%vEnter: %T\n", this.GetIntend(this.Level), in)

	switch in.(type) {
	case *ast.DeleteTableList:
		this.CurrentBlock = DELETE_TABLE_LIST
	case *ast.BinaryOperationExpr:
		if this.CurrentBlock == DELETE_TABLE_LIST {
			this.CurrentBlock = DELETE_WHERE
			this.HasWhereClause = true
		}
	case *ast.BetweenExpr:
		if this.CurrentBlock == DELETE_TABLE_LIST {
			this.CurrentBlock = DELETE_WHERE
			this.HasWhereClause = true
		}
	case *ast.PatternInExpr:
		if this.CurrentBlock == DELETE_TABLE_LIST {
			this.CurrentBlock = DELETE_WHERE
			this.HasWhereClause = true
		}
	}

	this.Level ++
	return in, false
}

func (this *DeleteVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	this.Level --
	// fmt.Printf("%vLeave: %T\n", this.GetIntend(this.Level), in)

	return in, true
}

func (this *DeleteVisitor) GetIntend(_level int) string {
	var intend string
	for i := 0; i < _level * 4; i++ {
		intend += " "
	}

	return intend
}

