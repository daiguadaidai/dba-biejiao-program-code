package handler

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/server/form"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/reviewer"
	"github.com/liudng/godump"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/parser"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/ast"
)

func ReviewSqlHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	//    r.Body.Close()
	body_str := string(body)
	fmt.Println(body_str)

	responsData := NewResponseData()

	sqlReviewForm := new(form.SqlReviewForm)
	if err := json.Unmarshal(body, sqlReviewForm); err != nil {
		responsData.Code = 2
		reviewMSG := reviewer.NewReviewMSG()
		reviewMSG.HaveError = true
		errMSG := fmt.Sprintf("传入参数有误: %v", err)
		reviewMSG.AppendMSG(reviewMSG.HaveError, errMSG)
		responsData.ReviewMSGs = append(responsData.ReviewMSGs, reviewMSG)
		fmt.Fprintln(w, responsData.ToJson())
		return
	} else {
		godump.Dump(sqlReviewForm)
		dbConfig := config.NewDBConfig(sqlReviewForm.Host, sqlReviewForm.Port,
			sqlReviewForm.Username, sqlReviewForm.Password, sqlReviewForm.Database)

		sqlParser := parser.New()

		stmtNodes, err := sqlParser.Parse(sqlReviewForm.Sqls, "", "")
		if err != nil {
			responsData.Code = 2
			reviewMSG := reviewer.NewReviewMSG()
			reviewMSG.HaveError = true
			errMSG :=fmt.Sprintf("语法解析出错: %v", err)
			reviewMSG.AppendMSG(reviewMSG.HaveError, errMSG)
			responsData.ReviewMSGs = append(responsData.ReviewMSGs, reviewMSG)
			fmt.Fprintln(w, responsData.ToJson())
			return
		}

		for _, stmtNode := range stmtNodes {
			switch stmtNode.(type) {
			case *ast.CreateTableStmt:
				createTableReviewer := &reviewer.CreateTableReviewer {
					ReviewConfig: config.NewRviewConfig(),
					StmtNode    : stmtNode.(*ast.CreateTableStmt),
					ReviewMSG   : reviewer.NewReviewMSG(),
					DBConfig    : dbConfig,
				}

				reviewMSG := createTableReviewer.Review()
				responsData.ReviewMSGs = append(responsData.ReviewMSGs, reviewMSG)
			case *ast.DeleteStmt:
				deleteReviewer := &reviewer.DeleteReviewer {
					ReviewConfig: config.NewRviewConfig(),
					StmtNode    : stmtNode.(*ast.DeleteStmt),
					ReviewMSG   : reviewer.NewReviewMSG(),
					DBConfig    : dbConfig,
				}

				reviewMSG := deleteReviewer.Review()
				responsData.ReviewMSGs = append(responsData.ReviewMSGs, reviewMSG)
			}
		}

		responsData.ResetCode()
		fmt.Fprintln(w, responsData.ToJson())
		return
	}
}
