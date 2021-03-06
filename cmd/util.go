package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/qiniu/log"
)

var (
	targetDateFormat = "2006-01-02T15:04:05-0700"
	NL               = []byte{'\n'}
	ANT              = []byte{'#'}
)

func normalizeDate(str string) (t time.Time, err error) {
	// 没有指定时区，格式化为本地区域，中国大陆为东八区 +0800
	dfs := []string{
		"20060102T15:04", "20060102T15:04:05",
		"2006-01-02T15:04:05", "2006-01-02 15:04:05"}
	for _, df := range dfs {
		t, err = time.ParseInLocation(df, str, time.Local)
		if err == nil {
			return
		}
	}
	// 指定了时区
	dfs = []string{"2006-01-02T15:04:05-07", "2006-01-02 15:04:05-07",
		"2006-01-02T15:04:05-0700", "2006-01-02 15:04:05-0700"}
	for _, df := range dfs {
		t, err = time.Parse(df, str)
		if err == nil {
			return
		}
	}
	return t, fmt.Errorf(" %s : %s ", "时间格式不正确", str)
}

// 移除 json 配置文件的注释部分，然后执行 json 解析
func loadEx(conf interface{}, configFilePath *string) (err error) {
	data, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		return
	}
	data = trimComments(data)

	err = json.Unmarshal(data, conf)
	if err != nil {
		log.Errorf("Parse conf %v failed: %v", string(data), err)
	}
	return
}

func trimComments(data []byte) (data1 []byte) {
	conflines := bytes.Split(data, NL)
	for k, line := range conflines {
		conflines[k] = trimCommentsLine(line)
	}
	return bytes.Join(conflines, NL)
}

// 移除 行 的注释部分
func trimCommentsLine(line []byte) []byte {
	var newLine []byte
	var i, quoteCount int
	lastIdx := len(line) - 1
	for i = 0; i <= lastIdx; i++ {
		// 排除转移符 \\  \" ，\" 不表示引号，不影响 quoteCount 统计
		if line[i] == '\\' {
			if i != lastIdx && (line[i+1] == '\\' || line[i+1] == '"') {
				newLine = append(newLine, line[i], line[i+1])
				i++
				continue
			}
		}
		if line[i] == '"' {
			quoteCount++
		}
		if line[i] == '#' {
			// 引号成对出现，若出现双数个，表示 # 不在 引号 内，即 从 i 开始，后续内容为注释；
			// 注释内容直接丢弃，即新行的内容不包含这些内容。
			if quoteCount%2 == 0 {
				break
			}
		}
		newLine = append(newLine, line[i])
	}
	return newLine
}
