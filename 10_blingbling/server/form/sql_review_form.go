package form

type SqlReviewForm struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Sqls     string `json:"sqls"`
}
