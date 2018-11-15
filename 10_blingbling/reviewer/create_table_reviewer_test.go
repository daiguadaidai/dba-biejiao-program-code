package reviewer

import (
	"testing"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/parser"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"github.com/liudng/godump"
)

func TestCreateTableReviewer_Review_TableNameReg(t *testing.T) {
	sql := `
CREATE TABLE 1pilipala_run_task (
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
		switch stmtNode.(type) {
		case *ast.CreateTableStmt:
			createTableReviewer := CreateTableReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.CreateTableStmt),
				ReviewMSG: NewReviewMSG(),
			}

			reviewMSG := createTableReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}

func TestCreateTableReviewer_Review_AllowNotNotNull(t *testing.T) {
	sql := `
CREATE TABLE pilipala_run_task (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) COMMENT '任务的UUID',
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
		switch stmtNode.(type) {
		case *ast.CreateTableStmt:
			createTableReviewer := CreateTableReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.CreateTableStmt),
				ReviewMSG: NewReviewMSG(),
			}

			reviewMSG := createTableReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}

func TestCreateTableReviewer_Review_IndexNameReg(t *testing.T) {
	sql := `
CREATE TABLE pilipala_run_task (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) COMMENT '任务的UUID',
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
    INDEX pid(pid),
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
		switch stmtNode.(type) {
		case *ast.CreateTableStmt:
			createTableReviewer := CreateTableReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.CreateTableStmt),
				ReviewMSG: NewReviewMSG(),
			}

			reviewMSG := createTableReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}

func TestCreateTableReviewer_Review_DuplicateIndex(t *testing.T) {
	sql := `
CREATE TABLE  (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) COMMENT '任务的UUID',
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
    INDEX idx_task_uuid_pid(task_uuid, pid),
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
		switch stmtNode.(type) {
		case *ast.CreateTableStmt:
			createTableReviewer := CreateTableReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.CreateTableStmt),
				ReviewMSG: NewReviewMSG(),
			}

			reviewMSG := createTableReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}

// 表是否存在测试
func TestCreateTableReviewer_Review_TableExists(t *testing.T) {
	sql := `
CREATE TABLE departments (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    pilipala_command_program_id BIGINT NOT NULL COMMENT '关联命令ID',
    pilipala_host_id BIGINT NOT NULL COMMENT '任务所在host',
    task_uuid VARCHAR(20) COMMENT '任务的UUID',
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
    INDEX idx_task_uuid_pid(task_uuid, pid),
    INDEX idx_created_at(created_at),
    INDEX idx_updated_at(updated_at)
)COMMENT='运行的任务';
`

	sqlParser := parser.New()

	stmtNodes, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		t.Errorf("语法解析出错: %v", err)
	}

	dbConfig := config.NewDBConfig(
		"10.10.10.21",
		3307,
		"HH",
		"oracle12",
		"employees")

	for _, stmtNode := range stmtNodes {
		switch stmtNode.(type) {
		case *ast.CreateTableStmt:
			createTableReviewer := CreateTableReviewer{
				ReviewConfig: config.NewRviewConfig(),
				StmtNode: stmtNode.(*ast.CreateTableStmt),
				ReviewMSG: NewReviewMSG(),
				DBConfig: dbConfig,
			}

			reviewMSG := createTableReviewer.Review()
			godump.Dump(reviewMSG)
		}
	}

}
