package handler

import (
	"testing"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/reviewer"
	"fmt"
)

func TestResponseData_ToJson(t *testing.T) {
	responseData := NewResponseData()

	reviewMSG1 := reviewer.NewReviewMSG()
	reviewMSG1.Sql = "delete from t1 where id=1"
	reviewMSG1.HaveWarning = true
	reviewMSG1.AppendMSG(reviewMSG1.HaveError, "第一个警告")

	reviewMSG2 := reviewer.NewReviewMSG()
	reviewMSG2.Sql = "update from t1 where id=1"
	reviewMSG2.HaveError = true
	reviewMSG2.AppendMSG(reviewMSG2.HaveError, "第一个错误")

	responseData.ReviewMSGs = append(responseData.ReviewMSGs, reviewMSG1, reviewMSG2)
	responseData.ResetCode()

	fmt.Println(responseData.ToJson())
}
