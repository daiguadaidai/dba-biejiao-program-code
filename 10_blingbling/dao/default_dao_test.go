package dao

import (
	"testing"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"fmt"
)

func GetDefaultDaoTest() *DefaultDao {
	dbConfig := config.NewDBConfig(
		"10.10.10.21",
		3307,
		"HH",
		"oracle12",
		"employees")
	defaultDao := NewDefaultDao(dbConfig)

	return defaultDao
}

func TestDefaultDao_TableExists(t *testing.T) {
	defaultDao := GetDefaultDaoTest()

	if err := defaultDao.Instance.OpenDB(); err != nil {
		t.Errorf("打开数据库出错. %v", err)
		return
	}
	defer defaultDao.Instance.CloseDB()

	exists, err := defaultDao.TableExists("", "depnts")
	if err != nil {
		t.Errorf("获取表是否存在出错. %v", err)
		return
	}

	if exists {
		fmt.Println("表已经存在", exists)
	} else {
		fmt.Println("表不存在", exists)
	}
}

func TestDefaultDao_GetExplainMaxRows(t *testing.T) {
	defaultDao := GetDefaultDaoTest()

	sql := `
explain select * from employees.departments
`
	if err := defaultDao.Instance.OpenDB(); err != nil {
		t.Errorf("打开数据库出错. %v", err)
		return
	}
	defer defaultDao.Instance.CloseDB()

	count, err := defaultDao.GetExplainMaxRows(sql)
	if err != nil {
		t.Errorf("获取表是否存在出错. %v", err)
		return
	}

	fmt.Println("最大影响行数为:", count)
}
