package api

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/logdb"
)

const (
	DateLayout = "2006-01-02T15:04:05-0700"
)

type CtlArg struct {
	Fields    string    // 显示展示哪些字段，* 表示全部字段。字段名以逗号 , 分割，忽略空格
	ShowIndex bool      // 是否显示行号
	Split     string    // 显示时，各字段的分割方式
	DateField string    //时间范围所作用的字段，如 timestamp
	Sort      string    // 排序方式 desc 或 asc 。按时间字段排序
	Start     time.Time // 查询的起始时间
	End       time.Time // 查询的结束时间
	PreSize   int       // 每次查询多少条
	Scroll    bool      // 是否使用 scroll 方式拉取数据
	fields    []logdb.RepoSchemaEntry
}

type Config struct {
	Ak    string   `json:"ak"`
	Sk    string   `json:"sk"`
	Repo  []string `json:"repo"`
	Debug bool     `json:"debug"`
}

func ListRepos(conf *Config, verbose bool) (err error) {
	logdbClient, err := buildClient(conf)
	if err != nil {
		return
	}
	repos, err := (*logdbClient).ListRepos(&logdb.ListReposInput{})
	if err != nil {
		return
	}
	err = showRepos(repos, verbose)
	return
}

func showRepos(repos *logdb.ListReposOutput, verbose bool) (err error) {
	sort.Slice(repos.Repos, func(i, j int) bool {
		return repos.Repos[i].RepoName < repos.Repos[j].RepoName
	})

	iLen := 10 // 字段显示所占长度
	for _, v := range repos.Repos {
		l := len(v.RepoName)
		if l > iLen {
			iLen = l
		}
	}
	iLen += 2
	sLen := strconv.Itoa(iLen)
	for i, v := range repos.Repos {
		if verbose {
			fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s\n",
				i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
		} else {
			fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\n",
				i, v.RepoName, v.Region, v.Retention)
		}
	}
	return
}

func QuerySample(conf *Config) (err error) {
	logdbClient, err := buildClient(conf)
	if err != nil {
		return
	}
	qstr := "*"
	logs, err := doQuery(logdbClient, conf, &qstr, "", 1, false)
	if err != nil {
		return
	}
	if logs != nil && len(logs.Data) > 0 {
		repoInfo, err1 := getRepoInfo(logdbClient, conf)
		if err1 != nil {
			return err1
		}
		showSample(conf, logs, repoInfo)
	}
	return
}

func showSample(conf *Config, logs *logdb.QueryLogOutput, repoInfo *logdb.GetRepoOutput) {
	if logs != nil && len(logs.Data) > 0 {
		fields, maxFiledLen := getShowFields("*", repoInfo)
		fmt.Printf("%s\n", formatDbLog(&logs.Data[0], &fields, "\n", maxFiledLen))
	}
}

func getShowFields(fieldsStr string, repo *logdb.GetRepoOutput) ([]logdb.RepoSchemaEntry, int) {
	tempMaxFiledLen := 5
	tempFields := []logdb.RepoSchemaEntry{}
	for _, e := range repo.Schema {
		s := len(e.Key)
		if s > tempMaxFiledLen {
			tempMaxFiledLen = s
		}
		tempFields = append(tempFields, e)
	}
	if fieldsStr == "*" {
		return tempFields, tempMaxFiledLen
	}

	maxFiledLen := 5
	fields := []logdb.RepoSchemaEntry{}
	for _, v := range strings.Split(fieldsStr, ",") {
		v = strings.TrimSpace(v)
		if "*" == v {
			fields = append(fields, tempFields...)
			maxFiledLen = tempMaxFiledLen
		} else {
			field := getField(tempFields, v)
			if field != nil {
				s := len(v)
				if s > maxFiledLen {
					maxFiledLen = s
				}
				fields = append(fields, *field)
			}
		}
	}
	return fields, maxFiledLen
}

func getField(fields []logdb.RepoSchemaEntry, key string) *logdb.RepoSchemaEntry {
	for _, v := range fields {
		if v.Key == key {
			return &v
		}
	}
	return nil
}

func formatDbLog(entity *map[string]interface{}, fields *[]logdb.RepoSchemaEntry,
	split string, maxFiledLen int) string {
	verbose := maxFiledLen > 0
	formatf := warpRed("%"+strconv.Itoa(maxFiledLen)+"s:") + "\t%.0f"
	formatv := warpRed("%"+strconv.Itoa(maxFiledLen)+"s:") + "\t%v"
	values := []string{}
	for _, entry := range *fields {
		field := entry.Key
		v := (*entity)[field]
		// "valtype":"long"  被转换为 float64 ,显示不友好，单独格式化
		switch entry.ValueType {
		case "long":
			if verbose {
				s := fmt.Sprintf(formatf, field, v)
				values = append(values, replaceNewline(&s))
			} else {
				s := fmt.Sprintf("%.0f", v)
				values = append(values, replaceNewline(&s))
			}
			break
		default:
			if verbose {
				s := fmt.Sprintf(formatv, field, v)
				values = append(values, replaceNewline(&s))
			} else {
				s := fmt.Sprint(v)
				values = append(values, replaceNewline(&s))
			}
		}
	}
	return strings.Join(values, split)
}

func replaceNewline(ps *string) string {
	s := *ps
	s = strings.Replace(s, "\r\n", "\\n", -1)
	s = strings.Replace(s, "\n", "\\n", -1)
	s = strings.Replace(s, "\r", "\\n", -1)
	return s
}

func warpRed(s string) string {
	return fmt.Sprintf("\033[0;31m%s\033[0m", s)
}

func Query(conf *Config, query string, arg *CtlArg) (err error) {
	logdbClient, err := buildClient(conf)
	if err != nil {
		return
	}
	repoInfo, err := getRepoInfo(logdbClient, conf)
	if err != nil {
		return
	}
	warn := checkInRetention(&arg.Start, &arg.End, strings.ToLower(repoInfo.Retention))
	log.Warn(warn)
	sort := buildQueryStr(logdbClient, conf, repoInfo, &query, arg)
	err = execQuery(logdbClient, conf, repoInfo, &query, arg, sort)
	return
}

func getRepoInfo(logdbClient *logdb.LogdbAPI, conf *Config) (repoInfo *logdb.GetRepoOutput, err error) {
	repoInfo, err = (*logdbClient).GetRepo(&logdb.GetRepoInput{RepoName: conf.Repo[0]})
	if err != nil {
		repoInfo, err = (*logdbClient).GetRepo(&logdb.GetRepoInput{RepoName: conf.Repo[0]})
	}
	return
}

// retention :eg: 7d, 30d
func checkInRetention(start, end *time.Time, retention string) (warn error) {
	//forever storage
	if strings.TrimSpace(retention) == "-1" {
		return
	}
	day := 0
	for _, c := range retention {
		if unicode.IsDigit(c) {
			// ascii , 48 ==> 0
			day = day*10 + int(c-'0')
		} else {
			break
		}
	}

	earliest := time.Now().Add(-time.Hour * 24 * time.Duration(day))
	if earliest.After(*end) {
		warn = fmt.Errorf("[%v ~ %v]时间范围太过久远，建议在 \"%s\" 之内。", start, end, retention)
		return
	}

	if earliest.After(*start) {
		warn = fmt.Errorf("[%v ~ %v]时间可能超出范围，不一定能获取到有效数据。最好在 \"%s\" 之内。", start, end, retention)
	}
	return
}

func buildQueryStr(logdbClient *logdb.LogdbAPI, conf *Config,
	repoInfo *logdb.GetRepoOutput, pquery *string, arg *CtlArg) (sort string) {
	dateField, sort := getDateFieldAndSort(logdbClient, conf, repoInfo, &arg.DateField, &arg.Sort)
	if len(dateField) != 0 {
		query := *pquery
		if len(query) != 0 {
			query = "(" + query + ") AND "
		}
		query += dateField + ":[" + arg.Start.Format(DateLayout) +
			" TO " + arg.End.Format(DateLayout) + "]"
		*pquery = query
	}
	return
}

func getDateFieldAndSort(logdbClient *logdb.LogdbAPI, conf *Config,
	repoInfo *logdb.GetRepoOutput, dateField, order *string) (string, string) {
	if len(*dateField) > 0 {
		return *dateField, *dateField + ":" + *order
	}

	for _, e := range repoInfo.Schema {
		if e.ValueType == "date" {
			return e.Key, e.Key + ":" + *order
		}
	}
	return "", ""
}

func execQuery(logdbClient *logdb.LogdbAPI, conf *Config, repoInfo *logdb.GetRepoOutput,
	query *string, arg *CtlArg, sort string) (err error) {
	logs, err := doQuery(logdbClient, conf, query, sort, arg.PreSize, arg.Scroll)
	if err != nil {
		log.Error(err)
		return
	}

	log.Debugf("FirstQuery: [scroll: %v...(%v), total:%v, state:%v, size: %v]\n", logs.ScrollId[:MinInt(23, len(logs.ScrollId))], len(logs.ScrollId), logs.Total, logs.PartialSuccess, len(logs.Data))

	size := len(logs.Data)
	total := size
	showLogs(conf, repoInfo, logs, arg, 1)
	logs.Data = nil

	for logs.Total > total && len(logs.ScrollId) > 1 && size > 0 {
		scrollInput := &logdb.QueryScrollInput{
			RepoName: conf.Repo[0],
			ScrollId: logs.ScrollId,
			Scroll:   "3m",
		}
		logs, err = (*logdbClient).QueryScroll(scrollInput)
		if err != nil {
			log.Error(err)
			return
		}
		log.Debugf("scroll: %v, logstotal:%v, state:%v, size: %v, total: %v\n", logs.ScrollId, logs.Total, logs.PartialSuccess, size, total)
		showLogs(conf, repoInfo, logs, arg, total)
		size = len(logs.Data)
		total += size
		logs.Data = nil
		err = nil
	}
	return
}

func showLogs(conf *Config, repoInfo *logdb.GetRepoOutput, logs *logdb.QueryLogOutput, arg *CtlArg, from int) {
	if arg.fields == nil || len(arg.fields) == 0 {
		arg.fields, _ = getShowFields(arg.Fields, repoInfo)
	}

	if arg.ShowIndex {
		for i, v := range logs.Data {
			fmt.Printf("%d\t%s\n", i+from, formatDbLog(&v, &arg.fields, arg.Split, -1))
		}
	} else {
		for _, v := range logs.Data {
			fmt.Println(formatDbLog(&v, &arg.fields, arg.Split, -1))
		}
	}
}

func QueryReqid(conf *Config, reqid string, reqidField string, arg *CtlArg) (err error) {
	log.Debugf("%v: %v", reqidField, reqid)
	unixNano, err := parseReqid(reqid)
	if err != nil {
		err = fmt.Errorf("reqid：%v 格式不正确：%v", reqid, err)
		return
	}
	logdbClient, err := buildClient(conf)
	if err != nil {
		return
	}
	var repoInfo *logdb.GetRepoOutput
	if len(reqidField) == 0 {
		repoInfo, err = getRepoInfo(logdbClient, conf)
		if err != nil {
			return
		}
		reqidField = getReqidField(repoInfo, "reqid", "respheader")
	}

	if len(reqidField) == 0 {
		err = errors.New("没有找到合适的字段用于查询 reqid，请使用 --reqidField <reqidField> 指定字段")
		return
	}
	query := reqidField + ":" + reqid
	t := time.Unix(unixNano/1e9, 0)
	arg.Start = t.Add(-time.Minute * 3)
	arg.End = t.Add(time.Minute * 10)
	logs, err := doQuery(logdbClient, conf, &query, "", 1000, arg.Scroll)
	if err != nil {
		return
	}

	log.Debugf("FirstQuery: [scroll: %v...(%v), total:%v, state:%v, size: %v]\n", logs.ScrollId[:MinInt(23, len(logs.ScrollId))], len(logs.ScrollId), logs.Total, logs.PartialSuccess, len(logs.Data))

	if len(logs.Data) > 0 {
		if repoInfo == nil {
			repoInfo, err = getRepoInfo(logdbClient, conf)
			if err != nil {
				return
			}
		}
		showLogs(conf, repoInfo, logs, arg, 1)
	}
	return
}

func parseReqid(reqid string) (unixNano int64, err error) {
	data, err := base64.URLEncoding.DecodeString(reqid)
	if err != nil {
		return
	}
	if len(data) != 12 {
		err = errors.New("invalid reqId")
		return
	}
	unixNano = int64(binary.LittleEndian.Uint64(data[4:]))
	return
}

func getReqidField(repoInfo *logdb.GetRepoOutput, fields ...string) string {
	for _, field := range fields {
		for _, e := range repoInfo.Schema {
			if strings.ToLower(e.Key) == field {
				return e.Key
			}
		}
	}
	return ""
}

func doQuery(logdbClient *logdb.LogdbAPI, conf *Config, qstr *string, sort string,
	size int, srcoll bool) (logs *logdb.QueryLogOutput, err error) {
	if len(conf.Repo) == 0 {
		err = errors.New("ERROR: HAVE NOT set repo ")
		return
	}
	queryInput := &logdb.QueryLogInput{
		RepoName: conf.Repo[0],
		Query:    *qstr, //query字段sdk会自动做url编码，用户不需要关心
		Sort:     sort,
		From:     0,
		Size:     size,
	}
	if srcoll {
		queryInput.Scroll = "3m"
	}

	log.Debugf("%+v\n", *queryInput)
	return (*logdbClient).QueryLog(queryInput)
}

func buildClient(conf *Config) (*logdb.LogdbAPI, error) {
	cfg := logdb.NewConfig().
		WithAccessKeySecretKey(conf.Ak, conf.Sk).
		WithEndpoint("https://logdb.qiniu.com").
		WithDialTimeout(30 * time.Second).
		WithResponseTimeout(120 * time.Second).
		WithLogger(base.NewDefaultLogger()).
		WithLoggerLevel(base.LogDebug)
	client, err := logdb.New(cfg)
	return &client, err
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
