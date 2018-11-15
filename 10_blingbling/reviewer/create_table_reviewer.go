package reviewer

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
	"regexp"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"fmt"
	"crypto/md5"
	"strings"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/dao"
)

type CreateTableReviewer struct {
	StmtNode *ast.CreateTableStmt
	ReviewMSG *ReviewMSG
	ReviewConfig *config.ReviewConfig
	DBConfig *config.DBConfig

	ConstaintColumns map[string][]string
}

func (this *CreateTableReviewer) Init() {
	this.ConstaintColumns = make(map[string][]string)
}

func (this *CreateTableReviewer) Review() *ReviewMSG {
	this.Init()


	haveError := this.DetectTableNameReg(this.StmtNode.Table.Name.String())
	if haveError {
		return this.ReviewMSG
	}

	haveError = this.DetectColumnNotNull()
	if haveError {
		return this.ReviewMSG
	}

	haveError = this.DetectIndexNameReg()
	if haveError {
		return this.ReviewMSG
	}

	haveError = this.DetectDuplicateIndex()
	if haveError {
		return this.ReviewMSG
	}

	haveError = this.DetectTableNameExists()
	if haveError {
		return this.ReviewMSG
	}

	return this.ReviewMSG
}

// 检测表名规则
func (this *CreateTableReviewer) DetectTableNameReg(_name string) (haveError bool) {

	match, _ := regexp.MatchString(this.ReviewConfig.RuleNameReg, _name)
	if !match {
		haveError = true
		this.ReviewMSG.AppendMSG(haveError,
			fmt.Sprintf("表名不符合规则. 表名：%v. 规则为: %v", _name, this.ReviewConfig.RuleNameReg))
		return
	}

	return
}

// 审核字段是否为空
func (this *CreateTableReviewer) DetectColumnNotNull() (haveError bool) {
	for _, col := range this.StmtNode.Cols {
		var haveNotNull bool
		for _, option := range col.Options {
			switch option.Tp {
			case ast.ColumnOptionNotNull:
				haveNotNull = true
			}
		}
		// 不允许null, 有NULL
		if !this.ReviewConfig.RuleAllowColumnNull && !haveNotNull {
			haveError = true
			this.ReviewMSG.AppendMSG(haveError,
				fmt.Sprintf("字段:%v 不允许为NULL", col.Name.String()))
			return
		}
	}

	return
}

// 检测索引名称是否合法
func (this *CreateTableReviewer) DetectIndexNameReg() (haveError bool) {
	for _, constraint := range this.StmtNode.Constraints {
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintUniq, ast.ConstraintUniqKey,
		ast.ConstraintUniqIndex:
			continue
		}
		match, _ := regexp.MatchString(this.ReviewConfig.RuleIndexNameReg, constraint.Name)
		if !match {
			haveError = true
			this.ReviewMSG.AppendMSG(haveError,
				fmt.Sprintf("索引不符合规则. 索引名：%v. 规则为: %v",
					constraint.Name, this.ReviewConfig.RuleIndexNameReg))
			return
		}
	}

	return
}

// 检测是否有重复索引
func (this *CreateTableReviewer) DetectDuplicateIndex() (haveError bool) {
	if this.ReviewConfig.RuleAllowDuplicateIndex {
		return
	}

	// 获取索引和字段名
	for _, constaint := range this.StmtNode.Constraints {
		switch constaint.Tp {
		case ast.ConstraintPrimaryKey:
			columns := make([]string, 0, 1)
			for _, column := range constaint.Keys {
				columns = append(columns, column.Column.String())
			}
			this.ConstaintColumns["PRIMARY"] = columns
		default:
			columns := make([]string, 0, 1)
			for _, column := range constaint.Keys {
				columns = append(columns, column.Column.String())
			}
			this.ConstaintColumns[constaint.Name] = columns
		}
	}

	// 将索引和字段名转化为hash值
	constraintColumnsHash := make(map[string]string)
	for indexName, columnNames := range this.ConstaintColumns {
		//将每个字段名生成hash保存到变量中
		columnNamesHash := make([]string, 0, 1)
		for _, columnName := range columnNames {
			data := []byte(columnName)
			has := md5.Sum(data)
			columnNameHash := fmt.Sprintf("%x", has) //将[]byte转成16进制
			columnNamesHash = append(columnNamesHash, columnNameHash)
		}

		constraintColumnsHash[indexName] = strings.Join(columnNamesHash, ",")
	}

	for indexNameOut, columnHashOut := range constraintColumnsHash {
		for indexNameIn, columnHashIn := range constraintColumnsHash {
			if indexNameIn == indexNameOut {
				continue
			}

			if strings.HasPrefix(columnHashOut, columnHashIn) {
				this.ReviewMSG.AppendMSG(haveError, fmt.Sprintf("发现重复索引: %v <=> %v",
					indexNameOut, indexNameIn))
			}
		}
	}

	return
}

func (this *CreateTableReviewer) DetectTableNameExists() (haveError bool) {

	defaultDao := dao.NewDefaultDao(this.DBConfig)

	if err := defaultDao.Instance.OpenDB(); err != nil {
		errMSG := fmt.Sprintf("语法正确, 无法打开数据库查看表是否存在. %v", err)
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}
	defer defaultDao.Instance.CloseDB()

	exists, err := defaultDao.TableExists(this.StmtNode.Table.Schema.String(),
		this.StmtNode.Table.Name.String())
	if err != nil {
		errMSG := fmt.Sprintf("语法正确, 查看表是否存在, 数据库端执行失败. %v", err)
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}

	if exists {
		haveError = true
		errMSG := fmt.Sprintf("表 %v 已经存存在", this.StmtNode.Table.Name.String())
		this.ReviewMSG.AppendMSG(haveError, errMSG)
		return
	}

	return
}
