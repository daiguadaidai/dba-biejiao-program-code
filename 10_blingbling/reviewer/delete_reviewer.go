package reviewer

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"fmt"
	"strings"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/dao"
)

type DeleteReviewer struct {
	StmtNode *ast.DeleteStmt
	ReviewMSG *ReviewMSG
	ReviewConfig *config.ReviewConfig
	DBConfig *config.DBConfig
	Visitor *DeleteVisitor
}

func (this *DeleteReviewer) Init() {
	this.Visitor = new(DeleteVisitor)
	this.StmtNode.Accept(this.Visitor)
}

func (this *DeleteReviewer) Review() *ReviewMSG {
	this.Init()

	haveError := this.DetectHasWhere()
	if haveError {
		return this.ReviewMSG
	}

	haveError = this.DetectAffectMaxRows()
	if haveError {
		return this.ReviewMSG
	}

	return this.ReviewMSG
}

func (this *DeleteReviewer) DetectHasWhere() (haveError bool) {
	fmt.Println(this.ReviewConfig.RuleAllowDeleteNoWhere, this.Visitor.HasWhereClause)
	if !this.ReviewConfig.RuleAllowDeleteNoWhere && !this.Visitor.HasWhereClause {
		haveError = true
		this.ReviewMSG.AppendMSG(haveError, "delete必须要有where条件")
		return
	}

	return
}

// 检测影响的最大行数
func (this *DeleteReviewer) DetectAffectMaxRows() (haveError bool) {
	defaultDao := dao.NewDefaultDao(this.DBConfig)
	if err := defaultDao.Instance.OpenDB(); err != nil {
		errMSG := fmt.Sprintf("打开数据库出错. %v", err)
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}
	defer defaultDao.Instance.CloseDB()

	count, err := defaultDao.GetExplainMaxRows(this.GetExplainSelectByDelete())
	if err != nil {
		haveError = true
		errMSG := fmt.Sprintf("获取表是否存在出错. %v", err)
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}

	if count > this.ReviewConfig.RuleAllowDeleteMaxRows {
		haveError = true
		errMSG := fmt.Sprintf("当前删除的行数(%v)超过阈值(%v)",
			count, this.ReviewConfig.RuleAllowDeleteMaxRows)
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}

	return
}

func (this *DeleteReviewer) GetExplainSelectByDelete() string {
	lowerDeletesql := strings.ToLower(this.StmtNode.Text())
	items := strings.SplitN(lowerDeletesql, "from", 2)

	return fmt.Sprintf("EXPLAIN SELECT * FROM %v", items[1])
}
