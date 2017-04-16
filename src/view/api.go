package view

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	base "qiniu.com/pandora/base"
	logdb "qiniu.com/pandora/logdb"
)

// CtlArg args
type CtlArg struct {
	Field  string
	Split  string
	Repo   string
	Sort   string
	Start  string
	End    string
	fields []string
	Head   int
	Debug  bool
}

type logCtlInfo struct {
	Range    int                  `json:"timeRange"`
	RepoName string               `json:"repoName"`
	Ak       string               `json:"ak"`
	Sk       string               `json:"sk"`
	Repo     *logdb.GetRepoOutput `json:"repo"`
}

var _info *logCtlInfo
var _client logdb.LogdbAPI

const _tempFile = ".qn_logdb_ctl_profile"

// Query by query and args
func Query(query *string, arg *CtlArg) {
	buildLogCtlInfo()

	// if info.RepoName != arg.RepoName {
	//
	// }

	if len(_info.RepoName) == 0 {
		fmt.Println(" == set repo first. ==")
		return
	}
	info := _info
	dateField := getDateField(info)
	sort := arg.Sort
	if len(sort) == 0 && len(dateField) > 0 {
		sort = dateField + ":desc"
	}
	if len(dateField) != 0 {
		buildQuery(query, arg, info, &dateField)
	}
	doQuery(query, arg, info, &sort, 0)
}

func doQuery(query *string, arg *CtlArg, info *logCtlInfo, sort *string, from int) {
	size := 200
	if (arg.Head > 0) && (arg.Head < size) {
		size = arg.Head
	}
	var _sort string
	if sort == nil {
		_sort = ""
	} else {
		_sort = *sort
	}
	queryInput := &logdb.QueryLogInput{
		RepoName: info.RepoName,
		Query:    *query, //query字段sdk会自动做url编码，用户不需要关心
		Sort:     _sort,
		From:     from,
		Size:     size,
	}

	if arg.Debug {
		log.Println(queryInput)
		log.Println()
	}

	err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}

	logs, err := _client.QueryLog(queryInput)
	if err != nil {
		log.Println(err)
		return
	}

	if arg.Debug {
		log.Println(logs.PartialSuccess)
		log.Println(logs.Total)
		log.Println()
	}
	showLogs(logs, arg, info, from)

	if logs.PartialSuccess || logs.Total > from+size {
		if size <= arg.Head {
			return
		}
		if (arg.Head > 0) && (from+size > arg.Head) {
			doQuery(query, arg, info, sort, arg.Head)
		} else {
			doQuery(query, arg, info, sort, from+size)
		}
	}
}

func getDateField(info *logCtlInfo) (dateField string) {
	for _, e := range info.Repo.Schema {
		if e.ValueType == "date" {
			dateField = e.Key
			return
		}
	}
	return ""
}

func buildQuery(query *string, arg *CtlArg, info *logCtlInfo, dateField *string) {
	iSLen := len(arg.Start)
	iELen := len(arg.End)
	end := arg.End
	// 未指定结束时间
	if iELen < 8 {
		end = time.Now().Format("2006-01-02T15:04:05+0800")
	}
	start := arg.Start
	// 只指定了结束时间
	if iSLen < 8 && iELen > 8 {
		start = "*"
		// 未指定时间
	} else if iSLen < 8 && iELen < 8 {
		if info.Range < 1 {
			info.Range = 5
		}
		start = time.Now().Add(-time.Duration(info.Range) * time.Minute).Format("2006-01-02T15:04:05+0800")
	}
	if len(*query) != 0 {
		*query += " AND "
	}
	*query += *dateField + ": [" + start + " TO " + end + "]"
}

func showLogs(logs *logdb.QueryLogOutput, arg *CtlArg, info *logCtlInfo, from int) {
	if arg.fields == nil || len(arg.fields) == 0 {
		arg.fields = getShowFields(arg, info)
	}

	for i, v := range logs.Data {
		fmt.Printf("%d\t%s\n", i+from, getLogStr(&v, &arg.fields, &arg.Split))
	}
}

func getLogStr(log *map[string]interface{}, fields *[]string, split *string) string {
	values := []string{}
	for _, filed := range *fields {
		v := (*log)[filed]
		// "valtype":"long"  被转换为 float64 ,显示不友好
		switch v.(type) {
		case float64, float32:
			values = append(values, fmt.Sprintf("%.0f", v))
			break
		default:
			values = append(values, fmt.Sprint(v))
		}

	}
	return strings.Join(values, *split)
}

func getShowFields(arg *CtlArg, info *logCtlInfo) []string {
	fields := []string{}
	for _, v := range strings.Split(arg.Field, ",") {
		v = strings.TrimSpace(v)
		if "*" == v {
			for _, e := range info.Repo.Schema {
				fields = append(fields, e.Key)
			}
		} else {
			fields = append(fields, v)
		}
	}
	return fields
}

// QueryHistogram query histogram
func QueryHistogram(query *string, arg *CtlArg) {
	buildLogCtlInfo()
	if len(_info.RepoName) == 0 {
		fmt.Println(" == set repo first. ==")
		return
	}
	info := _info
	dateField := arg.Field
	if len(dateField) == 0 {
		dateField = getDateField(info)
	}

	start, err := getTime(arg.Start)
	if err != nil {
		tr := info.Range
		if tr <= 0 {
			tr = 5
		}
		start = time.Now().Add(-time.Duration(tr) * time.Minute)
	}
	end, err := getTime(arg.End)
	if err != nil {
		end = time.Now()
	}
	histogramInput := &logdb.QueryHistogramLogInput{
		RepoName: info.RepoName,
		Query:    *query,
		From:     start.Unix() * 1000,
		To:       end.Unix() * 1000,
		Field:    dateField,
	}

	if arg.Debug {
		log.Println(arg)
		log.Println(start)
		log.Println(end)
		log.Println(info)
		log.Println(histogramInput)
		log.Println()
	}

	err = buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	histogramOutput, err := _client.QueryHistogramLog(histogramInput)
	if err != nil {
		log.Println(err)
	}
	if arg.Debug {
		log.Println(histogramOutput.Total)
		log.Println(histogramOutput.PartialSuccess)
		log.Println()
	}
	fmt.Println(histogramOutput)
}

func getTime(t string) (time.Time, error) {
	date, err := time.Parse("2006-01-02T15:04:05+0800", t)
	if err != nil {
		date, err = time.Parse("2006-01-02T15:04:05+08", t)
	}
	return date, err
}

//QueryReqid query logs by reqid
func QueryReqid(arg *CtlArg, reqidQuery string) {
	var reqidField string
	var reqid string
	ss := strings.Split(reqidQuery, ":")
	if len(ss) > 1 {
		reqidField = ss[0]
		reqid = ss[1]
	} else {
		reqidField = ""
		reqid = ss[0]
	}
	// 正确格式的 reqid
	unixNano, err := parseReqid(reqid)
	if err != nil {
		fmt.Println("reqid 格式不正确：", err)
		return
	}

	// 对应 repo 有效时间内的 reqid ，本地时间判断
	t := time.Unix(unixNano/1e9, 0)
	buildLogCtlInfo()

	day := 0
	for _, c := range _info.Repo.Retention {
		if unicode.IsDigit(c) {
			// ascii , 48 ==> 0
			day = day*10 + int(c-48)
		} else {
			break
		}
	}
	if arg.Debug {
		fmt.Printf("reqid: %s , time: %s , logdb 有效期 \"%s\" \n", reqid, t.Format("2006-01-02T15:04:05+0800"), _info.Repo.Retention)
	}
	if t.Unix() < (time.Now().Unix() - int64(day*(24*3600*1000))) {
		fmt.Printf("reqid: %s , time: %s 太过久远，要求在 \"%s\" 之内。\n", reqid, t.Format("2006-01-02T15:04:05+0800"), _info.Repo.Retention)
		return
	}
	// 构建查询语句，指定查询字段
	if len(reqidField) == 0 {
		reqidField = getReqidField(_info, "reqid")
	}
	if len(reqidField) == 0 {
		reqidField = getReqidField(_info, "respheader")
	}
	if len(reqidField) == 0 {
		fmt.Printf("没有找到合适的字段用于查询 reqid，请使用  <reqidField>:<reqid> 指定字段 \n")
		return
	}
	query := reqidField + ":" + reqid
	doQuery(&query, arg, _info, nil, 0)
}

func getReqidField(info *logCtlInfo, field string) string {
	for _, e := range info.Repo.Schema {
		if e.Key == field {
			return e.Key
		}
	}
	return ""
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

// Login by ak and sk
func Login(ak string, sk string) {
	info := &logCtlInfo{}
	info.Ak = ak
	info.Sk = sk
	info.Range = 5
	storeInfo(info)
}

// SetRepo set current repo
func SetRepo(repoName string) {
	buildLogCtlInfo()
	if len(repoName) != 0 {
		info := getInfoByName(repoName)
		if len(info.RepoName) != 0 {
			_info.RepoName = repoName
			_info.Repo = info.Repo
			storeInfo(_info)
		} else {
			fmt.Printf("Set repo: %s failed, Nothing changed.\n", repoName)
		}
	}
	showRepo(_info)
}

// SetTimeRange set range
func SetTimeRange(r int) {
	buildLogCtlInfo()
	_info.Range = r
	storeInfo(_info)
}

// ListRepos list repos
func ListRepos(verbose bool) {
	buildLogCtlInfo()
	err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	repos, err := _client.ListRepos(&logdb.ListReposInput{}) // 列举repo
	if err != nil {
		log.Println(err)
		return
	}

	iLen := 10
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
			if _info.RepoName == v.RepoName {
				fmt.Printf(warpRed("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s **")+"\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			} else {
				fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			}
		} else {
			if _info.RepoName == v.RepoName {
				fmt.Printf(warpRed("%3d:  %-"+sLen+"s\t%s\t%s **")+"\n",
					i, v.RepoName, v.Region, v.Retention)
			} else {
				fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\n",
					i, v.RepoName, v.Region, v.Retention)
			}
		}
	}
}

// ShowRepo show repo
func ShowRepo(repoName string) {
	showRepo(getInfoByName(repoName))
}

func getInfoByName(repoName string) (info *logCtlInfo) {
	if len(repoName) == 0 {
		buildLogCtlInfo()
		return _info
	}
	err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	repo, err := _client.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
	if err != nil {
		log.Println(err)
		return
	}
	return &logCtlInfo{
		RepoName: repoName,
		Repo:     repo,
	}
}

func showRepo(info *logCtlInfo) {
	repo := info.Repo
	fmt.Printf("%11s: %s\n", "RepoName", info.RepoName)
	fmt.Printf("%11s: %s\n", "Region", repo.Region)
	fmt.Printf("%11s: %s\n", "Retention", repo.Retention)
	// fmt.Printf("%11s: %s\n", "CreateTime", repo.CreateTime)
	// fmt.Printf("%11s: %s\n", "UpdateTime", repo.UpdateTime)
	fmt.Printf("Field: (%d)\n", len(repo.Schema))
	// dateField := getDateField(info)
	var dateField string
	for _, e := range info.Repo.Schema {
		if e.ValueType == "date" && len(dateField) == 0 {
			dateField = e.Key
		}
		fmt.Println(e)
	}
	fmt.Printf("时间字段为： %s ，默认排序为： %s\n", warpRed(dateField), warpRed(dateField+":desc"))
}

func warpRed(s string) string {
	return fmt.Sprintf("\033[0;31m%s\033[0m", s)
}

func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func buildLogCtlInfo() (err error) {
	if _info == nil {
		data, err := ioutil.ReadFile(userHomeDir() + "/" + _tempFile)
		if err != nil {
			return err
		}
		_info = &logCtlInfo{}
		json.Unmarshal(data, _info)
	}
	return
}

func storeInfo(v *logCtlInfo) {
	bytes, err := json.Marshal(v)
	if err == nil && bytes != nil {
		err = ioutil.WriteFile(userHomeDir()+"/"+_tempFile, bytes, 0666)
		if err != nil {
			log.Println("Opps....", err)
		}
	}
}

func buildClient() (err error) {
	if _client == nil {
		buildLogCtlInfo()
		cfg := logdb.NewConfig().
			WithAccessKeySecretKey(_info.Ak, _info.Sk).
			WithEndpoint("https://logdb.qiniu.com").
			WithDialTimeout(30 * time.Second).
			WithResponseTimeout(120 * time.Second).
			WithLogger(base.NewDefaultLogger()).
			WithLoggerLevel(base.LogDebug)
		_client, err = logdb.New(cfg)
	}
	return
}
