package parser

import (
    "bufio"
    "fmt"
    "io"
    "os"
    "strings"

    "github.com/juju/errors"
)

const (
    FILE_GROUP = iota
    HOST_GROUP
)

type ArgParser struct {
    Hosts []string
    Sql   string
    File  string
}

// 解析命令行参数是否有指定 --file
func (self *ArgParser) fileGroupParse() (ok bool) {
    if len(self.File) > 0 {
        return true
    }

    return false
}

// 解析命令行参数是否有指定 --host-port
func (self *ArgParser) hostGroupParse() (ok bool) {
    if len(self.Hosts) > 0 {
        return true
    }

    return false
}

// 读取文件并且获取没一行数据
func (self *ArgParser) readFileHosts() ([]string, error) {
    f, err := os.Open(self.File)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    rd := bufio.NewReader(f)
    lines := make([]string, 1)
    for {
        line, err := rd.ReadString('\n')
        if err != nil {
            if io.EOF == err {
                break
            } else {
                continue
            }
        }

        line = strings.TrimSpace(line)
        lines = append(lines, line)
    }

    return lines, nil
}

// 解析校验命令行参数正确性
func (self *ArgParser) Parse() (groupType int64, err error) {
    if len(self.Sql) <= 3 {
        return -1, errors.New("没有输入正确的SQL语句, 请核实.")
    }

    if self.fileGroupParse() {
        return FILE_GROUP, nil
    } else if self.hostGroupParse() {
        return HOST_GROUP, nil
    }

    return -1, errors.New("没有获取到ip地址, 请核实.")
}

// 获取host-port数组
func (self *ArgParser) FindHosts(groupType int64) ([]string, error) {
    if groupType == FILE_GROUP {
        return self.readFileHosts()
    } else if groupType == HOST_GROUP {
        return self.Hosts, nil
    }

    return nil, errors.New(fmt.Sprintf("未知的命令行参数分组类型 %v.", groupType))
}
