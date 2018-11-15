package reviewer

import (
	"testing"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/parser"
	"github.com/liudng/godump"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
	"fmt"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
)

func TestDeleteReviwer_DeleteVisitor(t *testing.T) {

	sql := `
delete from t1 where id = 1 
`

	sqlParser := parser.New()
	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		visitor := new(DeleteVisitor)
		stmtNode.Accept(visitor)
		godump.Dump(visitor)

		fmt.Println("------------------------------------")
		deleteStmt := stmtNode.(*ast.DeleteStmt)
		godump.Dump(deleteStmt)
	}
}

// 检测delete是否有where子句
func TestDeleteReviewer_Review_HasWhere(t *testing.T) {
	sql := `
delete from t1
`

	sqlParser := parser.New()
	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		switch stmtNode.(type) {
		case *ast.DeleteStmt:
			deleteReviewer := &DeleteReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.DeleteStmt),
				ReviewMSG: NewReviewMSG(),
			}

			reviewMSG := deleteReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}

// delete sql 转化称select sql
func TestDeleteReviewer_GetExplainSelectByDelete(t *testing.T) {
	sql := `
delete t1 from (
    SELECT id, name FROM t2 where id = 1
) as t1 where id = (
    select id from t3 limit 1
)
`

	sqlParser := parser.New()
	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		switch stmtNode.(type) {
		case *ast.DeleteStmt:
			deleteReviewer := &DeleteReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.DeleteStmt),
				ReviewMSG: NewReviewMSG(),
			}

			fmt.Println(deleteReviewer.GetExplainSelectByDelete())
		}
	}

}

// delete sql 转化称select sql
func TestDeleteReviewer_DetectAffectMaxRows(t *testing.T) {
	dbConfig := config.NewDBConfig(
		"10.10.10.21",
		3307,
		"HH",
		"oracle12",
		"employees")

	sql := `
DELETE FROM dept_manager WHERE dept_no = 1
`

	sqlParser := parser.New()
	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		switch stmtNode.(type) {
		case *ast.DeleteStmt:
			deleteReviewer := &DeleteReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode    : stmtNode.(*ast.DeleteStmt),
				ReviewMSG   : NewReviewMSG(),
				DBConfig    : dbConfig,
			}

			reviewMSG := deleteReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}
