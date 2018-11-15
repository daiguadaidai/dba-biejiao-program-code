package example

import (
	"testing"
	"github.com/daiguadaidai/dba-biejiao-program-code/07_sql_parser/parser"
	"fmt"
	"github.com/daiguadaidai/dba-biejiao-program-code/07_sql_parser/ast"
	"github.com/daiguadaidai/dba-biejiao-program-code/07_sql_parser/dependency/mysql"
)

type CreateTableVisitorTest struct {
	Level int
}

func (this *CreateTableVisitorTest) GetIntend(_level int) string {
	var intend string
	for i := 0; i < _level * 4; i++ {
		intend += " "
	}

	return intend
}

func (this *CreateTableVisitorTest) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	fmt.Printf("%vEnter: %T\n", this.GetIntend(this.Level), in)

	this.Level ++
	return in, false
}

func (this *CreateTableVisitorTest) Leave(in ast.Node) (out ast.Node, ok bool) {
	this.Level --
	// fmt.Printf("%vLeave: %T\n", this.GetIntend(this.Level), in)

	return in, true
}

func TestParserTree(t *testing.T) {
	sql := `
CREATE TABLE pilipala_run_task (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) NOT NULL COMMENT '任务的UUID',
    host VARCHAR(50) NOT NULL COMMENT '任务运行在哪个host上',
    file_name VARCHAR(150) NOT NULL COMMENT '需要执行的程序名称',
    params VARCHAR(500) NOT NULL DEFAULT "" COMMENT "执行命令的参数",
    pid BIGINT NOT NULL DEFAULT 0 COMMENT '父任务id, 关联自己',
    notify_info VARCHAR(500) NOT NULL DEFAULT "" COMMENT "会实时读该字段的信息, 一般外部其他程序可以通过修改这个来和任务进行通讯.",
    real_info VARCHAR(500) NOT NULL DEFAULT "" COMMENT "保存了一些实时需要持久化的信息",
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY(id),
    UNIQUE INDEX udx_task_uuid(task_uuid),
    INDEX idx_pilipala_command_program_id(pilipala_command_program_id),
    INDEX idx_pid(pid),
    INDEX idx_created_at(created_at),
    INDEX idx_updated_at(updated_at)
)COMMENT='运行的任务';
`

	sqlParser := parser.New()

	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		visitor := new(CreateTableVisitorTest)
		stmtNode.Accept(visitor)
	}
}

func TestParserInfoUsage(t *testing.T) {
	sql := `
CREATE TABLE test.pilipala_run_task (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) NOT NULL COMMENT '任务的UUID',
    host VARCHAR(50) NOT NULL COMMENT '任务运行在哪个host上',
    file_name VARCHAR(150) NOT NULL COMMENT '需要执行的程序名称',
    params VARCHAR(500) NOT NULL DEFAULT "" COMMENT "执行命令的参数",
    pid BIGINT NOT NULL DEFAULT 0 COMMENT '父任务id, 关联自己',
    notify_info VARCHAR(500) NOT NULL DEFAULT "" COMMENT "会实时读该字段的信息, 一般外部其他程序可以通过修改这个来和任务进行通讯.",
    real_info VARCHAR(500) NOT NULL DEFAULT "" COMMENT "保存了一些实时需要持久化的信息",
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY(id),
    UNIQUE INDEX udx_task_uuid(task_uuid),
    INDEX idx_pilipala_command_program_id(pilipala_command_program_id),
    INDEX idx_pid(pid),
    INDEX idx_created_at(created_at),
    INDEX idx_updated_at(updated_at)
)COMMENT='运行的任务';
`

	sqlParser := parser.New()

	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	for _, stmtNode := range stmtNodes {
		createTableStmt := stmtNode.(*ast.CreateTableStmt)

		fmt.Printf("SchemaName: %v\n", createTableStmt.Table.Schema.String())
		fmt.Printf("TableName: %v\n", createTableStmt.Table.Name.String())

		for _, col := range createTableStmt.Cols {
			fmt.Println("------------ Col -----------")
			fmt.Println("ColumnName: ", col.Name.String())
			fmt.Println("ColumnType: ", col.Tp.String(), col.Tp.Tp)

			switch col.Tp.Tp {
			case mysql.TypeLonglong:
				fmt.Println("TypeLonglong")
			case mysql.TypeVarchar:
				fmt.Println("TypeVarchar")
			}

			for _, option := range col.Options {
				switch option.Tp {
				case ast.ColumnOptionNoOption:
					fmt.Println("ColumnOptionNoOption")
				case ast.ColumnOptionPrimaryKey:
					fmt.Println("ColumnOptionPrimaryKey")
				case ast.ColumnOptionNotNull:
					fmt.Println("ColumnOptionNotNull")
				case ast.ColumnOptionAutoIncrement:
					fmt.Println("ColumnOptionAutoIncrement")
				case ast.ColumnOptionDefaultValue:
					fmt.Println("ColumnOptionDefaultValue")
				case ast.ColumnOptionUniqKey:
					fmt.Println("ColumnOptionUniqKey")
				case ast.ColumnOptionNull:
					fmt.Println("ColumnOptionNull")
				case ast.ColumnOptionOnUpdate:
					fmt.Println("ColumnOptionOnUpdate")
				case ast.ColumnOptionFulltext:
					fmt.Println("ColumnOptionFulltext")
				case ast.ColumnOptionComment:
					fmt.Println("ColumnOptionComment")
				case ast.ColumnOptionGenerated:
					fmt.Println("ColumnOptionGenerated")
				case ast.ColumnOptionReference:
					fmt.Println("ColumnOptionReference")
				}

				if option.Expr != nil {
					fmt.Println("    ", option.Expr.GetValue())
				}
			}
		}

	}
}
