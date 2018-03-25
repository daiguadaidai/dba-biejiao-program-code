package imp

import (
    "database/sql"
    "fmt"
    "strconv"

    "03_go_batch-execute-sql/parser"

    _ "github.com/go-sql-driver/mysql"
)

func Start(argParser *parser.ArgParser, groupType int64) {
    hosts, err := argParser.FindHosts(groupType)
    if err != nil {
        panic(err)
    }

    for _, hostPort := range hosts {
        if hostPort == "" {
            continue
        }

        // 链接数据库
        dataSource := fmt.Sprintf("%v:%v@tcp(%v)/", "HH", "oracle", hostPort)
        db, err := sql.Open("mysql", dataSource)
        if err != nil {
            fmt.Printf("%v 链接数据库错误, %v\n", hostPort, err)
            db.Close()
            continue
        }

        // 执行结果
        rows, err := db.Query(argParser.Sql)
        if err != nil {
            fmt.Printf("%v 执行SQL错误, %v\n", hostPort, err)
            db.Close()
            continue
        }

        // 获取字段名
        columns, err := rows.Columns()
        if err != nil {
            fmt.Printf("%v 获取字段名错误, %v\n", hostPort, err)
            db.Close()
            continue
        }

        // 创建值, 用于赋值
        values := make([]sql.RawBytes, len(columns)) // 数据库原生二进制值
        scanArgs := make([]interface{}, len(values)) // 接收数据库原生二进制值，该值和上面定义的values进行关联
        colVals := make([]interface{}, len(values))  // 转换后的真实值
        for i := range values {
            scanArgs[i] = &values[i]
        }

        colTypes, err := rows.ColumnTypes()
        if err != nil {
            panic(err.Error()) // proper error handling instead of panic in your app
        }

        // Fetch rows
        for rows.Next() {
            // get RawBytes from data
            err = rows.Scan(scanArgs...)
            if err != nil {
                panic(err.Error()) // proper error handling instead of panic in your app
            }

            valFmtStr := ""
            for i, value := range values {
                data, fmtStr := RawBytes2Value(value, colTypes[i].DatabaseTypeName())
                colVals[i] = data
                valFmtStr += fmtStr + ", "
            }
            valFmtStr += "\n"
            fmt.Printf(valFmtStr, colVals...)
        }
        if err = rows.Err(); err != nil {
            panic(err.Error()) // proper error handling instead of panic in your app
        }

        db.Close()
    }
}

func RawBytes2Value(colBytes sql.RawBytes, colType string) (colVal interface{}, fmtStr string) {
    if colBytes == nil {
        return nil, "NULL"
    }
    switch colType {
    case "BIGINT":
        data, _ := strconv.ParseInt(string(colBytes), 10, 64)
        return data, "%d"
    case "VARCHAR":
        return string(colBytes), "%#v"
    case "INT":
        data, _ := strconv.ParseInt(string(colBytes), 10, 32)
        return data, "%d"
    }

    return nil, "NULL"
}
