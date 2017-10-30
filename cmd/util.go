package cmd

import (
	"fmt"
	"time"
)

var targetDateFormat = "2006-01-02T15:04:05-0700"

func normalizeDate(str string) (t time.Time, err error) {
	// 没有指定时区，格式化为 0800，默认为东八区
	dfs := []string{"20060102T15:04", "2006-01-02T15:04:05",
		"2006-01-02 15:04:05"}
	for _, df := range dfs {
		t, err = time.ParseInLocation(df, str, time.Local)
		if err == nil {
			return t, err
		}
	}
	// 指定了时区
	dfs = []string{"2006-01-02T15:04:05-07", "2006-01-02 15:04:05-07",
		"2006-01-02T15:04:05-0700", "2006-01-02 15:04:05-0700"}
	for _, df := range dfs {
		t, err = time.Parse(df, str)
		if err == nil {
			return t, err
		}
	}
	return t, fmt.Errorf(" %s : %s ", "时间格式不正确", str)
}
