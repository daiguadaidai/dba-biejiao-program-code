package dao

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/gdbc"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"strings"
	"fmt"
	"strconv"
)

type DefaultDao struct {
	Instance *gdbc.Instance
}

func NewDefaultDao(_dbConfig *config.DBConfig) *DefaultDao {
	defaultDao := new(DefaultDao)
	defaultDao.Instance = gdbc.NewInstance(_dbConfig)

	return defaultDao
}

// 判断表名称是否存在
func (this *DefaultDao) TableExists(_schema string, _table string) (bool, error) {
	if strings.TrimSpace(_schema) == "" {
		_schema = this.Instance.DBconfig.Database
	}

	sql := `
SELECT COUNT(*)
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ?
    AND TABLE_NAME = ?
    `

    var count int
    if err := this.Instance.DB.QueryRow(sql, _schema, _table).Scan(&count); err != nil {
		return false, err
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

// 获取explain中最大值
func (this *DefaultDao) GetExplainMaxRows(_sql string) (int, error) {
	var maxRowCount int
	var rowsColumnIndex int

	rows, err := this.Instance.DB.Query(_sql)
	if err != nil {
		errMSG := fmt.Sprintf("explain: %v:%v. %v %v",
			this.Instance.DBconfig.Host, this.Instance.DBconfig.Port, _sql, err)
		return -1, fmt.Errorf("%v", errMSG)
	}
	defer rows.Close()

	// rows
	columnNames, err := rows.Columns()
	if err != nil {
		errMSG := fmt.Sprintf(": %v:%v. %v %v",
			this.Instance.DBconfig.Host, this.Instance.DBconfig.Port, _sql, err)
		return -1, fmt.Errorf("%v", errMSG)
	}
	for i, columnName := range columnNames {
		if columnName == "rows" {
			rowsColumnIndex = i
		}
	}

	scanArgs := make([]interface{}, len(columnNames))
	values := make([]interface{}, len(columnNames))
	for j := range values {
		scanArgs[j] = &values[j]
	}
	for rows.Next() {
		rows.Scan(scanArgs...)
		var rowCount int
		if values[rowsColumnIndex] != nil {
			rowCount, err = strconv.Atoi(string(values[rowsColumnIndex].([]uint8)))
			if err != nil {
				errMSG := fmt.Sprintf("explain rows: %v:%v. %v %v",
					this.Instance.DBconfig.Host, this.Instance.DBconfig.Port, _sql, err)
				return -1, fmt.Errorf("%v", errMSG)
			}
		}
		if maxRowCount < rowCount {
			maxRowCount = rowCount
		}
	}

	err = rows.Err()
	if err != nil {
		errMSG := fmt.Sprintf("explain(scan): %v:%v. %v %v",
			this.Instance.DBconfig.Host, this.Instance.DBconfig.Port, _sql, err)
		return -1, fmt.Errorf("%v", errMSG)
	}

	return maxRowCount, nil
}


