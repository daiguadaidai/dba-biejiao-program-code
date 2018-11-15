package handler

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/reviewer"
	"encoding/json"
	"github.com/cihub/seelog"
)

type ResponseData struct {
	Code int
	ReviewMSGs []*reviewer.ReviewMSG
}

func NewResponseData() *ResponseData {
	responseData := new(ResponseData)
	responseData.ReviewMSGs = make([]*reviewer.ReviewMSG, 0, 1)

	return responseData
}

// 获取序列化json字符串
func (this *ResponseData) ToJson() string {
	jsons, err := json.Marshal(this) //转换成JSON返回的是byte[]
	if err != nil {
		seelog.Errorf("返回数据转化json出错. %v", err)
		return this.ErrorJson()
	}

	return string(jsons)
}

// 返回错误的json字符串
func (this *ResponseData) ErrorJson() string {
	return `
{
    "Code": 2,
    "ReviewMSGs": [
        {
            "Sql": "",
            "HaveError": true,
            "HaveWarning": false,
            "ErrorMSGs": [
                "返回数据变成json有错"
            ],
            "WarningMSGs": []
        }
    ]
}
`
}

// 设置返回信息的状态码
func (this *ResponseData) ResetCode() {
	for _, reviewMSG := range this.ReviewMSGs {
		if reviewMSG.HaveError {
			this.Code = 2
			return
		}
		if reviewMSG.HaveWarning {
			this.Code = 1
		}
	}
}
