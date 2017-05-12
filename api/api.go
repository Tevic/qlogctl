package api

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
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/qiniuts/logctl/crypto"
	"qiniu.com/pandora/base"
	"qiniu.com/pandora/logdb"
)

// CtlArg args
type CtlArg struct {
	Field  string
	Split  string
	Repo   string
	Sort   string
	Start  string
	End    string
	fields []logdb.RepoSchemaEntry
	Head   int
	Debug  bool
}

type logCtlInfo struct {
	User     string                  `json:"user"`
	Range    int                     `json:"timeRange"`
	RepoName string                  `json:"repoName"`
	Ak       string                  `json:"ak"`
	Sk       string                  `json:"sk"`
	Repo     *logdb.GetRepoOutput    `json:"repo"`
	Log      *map[string]interface{} `json:"log"`
}

var _info *logCtlInfo
var _logCtlCtx *logCtlCtx
var _client logdb.LogdbAPI

const _tempFile = ".qn_logdb_ctl_profile"

// Query by query and args
func Query(query *string, arg *CtlArg) {
	err := buildLogCtlInfo()
	if err != nil {
		log.Println(err)
		return
	}

	if len(_info.RepoName) == 0 {
		fmt.Println(" 请先设置要查询的 REPO ")
		return
	}
	info := _info
	dateField := getDateField(info.Repo)
	sort := arg.Sort
	if len(sort) == 0 && len(dateField) > 0 {
		sort = dateField + ":desc"
	}
	if len(dateField) != 0 {
		buildQuery(query, arg, info.Range, &dateField)
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
		log.Println("xxxxx", queryInput)
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

func getDateField(repo *logdb.GetRepoOutput) (dateField string) {
	for _, e := range repo.Schema {
		if e.ValueType == "date" {
			dateField = e.Key
			return
		}
	}
	return ""
}

func buildQuery(query *string, arg *CtlArg, defaultRange int, dateField *string) {
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
		if defaultRange < 1 {
			defaultRange = 5
		}
		start = time.Now().Add(-time.Duration(defaultRange) * time.Minute).Format("2006-01-02T15:04:05+0800")
	}
	if len(*query) != 0 {
		*query += " AND "
	}
	*query += *dateField + ": [" + start + " TO " + end + "]"
}

func showLogs(logs *logdb.QueryLogOutput, arg *CtlArg, info *logCtlInfo, from int) {
	if arg.fields == nil || len(arg.fields) == 0 {
		arg.fields = getShowFields(arg.Field, info.Repo)
	}

	for i, v := range logs.Data {
		fmt.Printf("%d\t%s\n", i+from, getLogStr(&v, &arg.fields, arg.Split, false))
	}
}

func getLogStr(log *map[string]interface{}, fields *[]logdb.RepoSchemaEntry, split string, verbose bool) string {
	values := []string{}
	for _, entry := range *fields {
		field := entry.Key
		v := (*log)[field]
		// "valtype":"long"  被转换为 float64 ,显示不友好
		switch entry.ValueType {
		case "long":
			if verbose {
				values = append(values, fmt.Sprintf(warpRed("%11s:")+"\t%.0f", field, v))
			} else {
				values = append(values, fmt.Sprintf("%.0f", v))
			}
			break
		default:
			if verbose {
				values = append(values, fmt.Sprintf(warpRed("%11s:")+"\t%v", field, v))
			} else {
				values = append(values, fmt.Sprint(v))
			}
		}

	}
	return strings.Join(values, split)
}

func getShowFields(fieldsStr string, repo *logdb.GetRepoOutput) []logdb.RepoSchemaEntry {
	temp := []logdb.RepoSchemaEntry{}
	for _, e := range repo.Schema {
		temp = append(temp, e)
	}
	if fieldsStr == "*" {
		return temp
	}

	fields := []logdb.RepoSchemaEntry{}
	for _, v := range strings.Split(fieldsStr, ",") {
		v = strings.TrimSpace(v)
		if "*" == v {
			fields = append(fields, temp...)
		} else {
			field := getField(temp, v)
			if field != nil {
				fields = append(fields, *field)
			}
		}
	}
	return fields
}

func getField(fields []logdb.RepoSchemaEntry, key string) *logdb.RepoSchemaEntry {
	for _, v := range fields {
		if v.Key == key {
			return &v
		}
	}
	return nil
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
		dateField = getDateField(info.Repo)
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
	err = buildLogCtlInfo()
	if err != nil {
		fmt.Println(err)
		return
	}

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
	dateField := getDateField(_info.Repo)
	if len(dateField) != 0 {
		start := t.Add(-2 * time.Minute).Format("2006-01-02T15:04:05+0800")
		end := t.Add(2 * time.Minute).Format("2006-01-02T15:04:05+0800")
		query += " AND " + dateField + ": [" + start + " TO " + end + "]"
	}
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

// SetRepo set current repo
func SetRepo(repoName string, refresh bool) {
	err := buildLogCtlInfo()
	if err != nil {
		log.Println(err)
		return
	}
	if !refresh {
		if len(repoName) == 0 && _info.Repo == nil {
			fmt.Println("Repo 为空，请指定 reponame")
			return
		}
		if len(repoName) == 0 || repoName == _info.RepoName {
			showRepo(_info)
			return
		}
		info, err := getCtlInfo(_logCtlCtx, _info.User, repoName)
		if err != nil {
			log.Println(err)
			return
		}
		if info != nil && info.Repo != nil {
			showRepo(info)
			storeInfo(info)
			return
		}
	}
	if len(repoName) == 0 {
		fmt.Println("RepoName 为空，查询失败")
		return
	}
	repo, sample, err := getNewInfoByName(repoName)
	if err != nil {
		log.Println(err)
		return
	}
	_info.RepoName = repoName
	_info.Repo = repo
	_info.Log = sample
	storeInfo(_info)
	showRepo(_info)
}

// QuerySample sample
func QuerySample() {
	err := buildLogCtlInfo()
	if err != nil {
		fmt.Println(err)
		return
	}
	if _info.Log == nil {
		sample, err := doQuerySample(_info.RepoName, _info.Repo)
		if err != nil {
			log.Println(err)
			return
		}
		_info.Log = sample
		storeInfo(_info)
	}
	// 显示样例
	if _info.Log != nil {
		fields := getShowFields("*", _info.Repo)
		fmt.Printf("%s\n", getLogStr(_info.Log, &fields, "\n", true))
	}
}

// Clear the cache
func Clear() {
	os.Remove(userHomeDir() + "/" + _tempFile)
}

// SetTimeRange set range
func SetTimeRange(r int) {
	err := buildLogCtlInfo()
	if err != nil {
		log.Println(err)
		return
	}
	_info.Range = r
	storeInfo(_info)
}

// ListRepos list repos
func ListRepos(verbose bool) (err error) {
	err = buildLogCtlInfo()
	if err != nil {
		log.Println(err)
		return
	}
	err = buildClient()
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
	return
}

func getNewInfoByName(repoName string) (repo *logdb.GetRepoOutput, sample *map[string]interface{}, err error) {
	err = buildClient()
	if err != nil {
		return
	}

	repo, err = _client.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
	if err != nil {
		return
	}
	sample, err = doQuerySample(repoName, repo)

	return
}

func doQuerySample(repoName string, repo *logdb.GetRepoOutput) (log *map[string]interface{}, err error) {
	dateField := getDateField(repo)
	query := ""
	start := time.Now().Add(-60 * time.Minute).Format("2006-01-02T15:04:05+0800")
	end := time.Now().Add(-55 * time.Minute).Format("2006-01-02T15:04:05+0800")
	buildQuery(&query, &CtlArg{Start: start, End: end}, 5, &dateField)
	queryInput := &logdb.QueryLogInput{
		RepoName: repoName,
		Query:    query,
		From:     0,
		Size:     1,
	}
	buildClient()
	logs, err := _client.QueryLog(queryInput)
	if err != nil {
		return nil, err
	}
	if len(logs.Data) > 0 {
		log = &logs.Data[0]
	}
	return
}

func showRepo(info *logCtlInfo) {
	repo := info.Repo
	fmt.Printf("\n%11s: %s\n", "User", info.User)
	// 显示 Repo 信息
	fmt.Printf("\n%11s: %s\n", "RepoName", info.RepoName)
	fmt.Printf("%11s: %s\n", "Region", repo.Region)
	fmt.Printf("%11s: %s\n", "Retention", repo.Retention)
	// 显示字段信息
	fmt.Printf("\nField: (%d)\n", len(repo.Schema))
	var dateField string
	for _, e := range repo.Schema {
		if e.ValueType == "date" && len(dateField) == 0 {
			dateField = e.Key
			fmt.Printf(warpRed("%v\n"), e)
		} else {
			fmt.Println(e)
		}
	}
	// 显示时间字段信息
	fmt.Printf("%s，时间字段为： %s ，默认排序为： %s\n", warpRed(info.RepoName), warpRed(dateField), warpRed(dateField+":desc"))

	// 显示样例
	if info.Log != nil {
		fmt.Println("\nSample: ")
		fields := getShowFields("*", repo)
		fmt.Printf("%s\n", getLogStr(info.Log, &fields, "\n", true))
	}
}

func warpRed(s string) string {
	return fmt.Sprintf("\033[0;31m%s\033[0m", s)
}

// Account the ak,sk and give them a name
func Account(ak string, sk string, name string) {
	info := buildUserContect(ak, sk, name)
	if info == _info && info.Repo != nil {
		showRepo(info)
		return
	}
	if info != _info {
		err := setCurrentUser(info)
		if err != nil {
			fmt.Println("设置 账号信息失败，请重试 ...")
			return
		}
	}
	err := ListRepos(false)
	if err == nil {
		fmt.Println(warpRed("请设置 REPO ..."))
	}
}

// Switch user account
func Switch(user string) {
	err := buildLogCtlInfo()
	if err != nil {
		log.Println(err)
		return
	}
	info, err := getCtlInfo(_logCtlCtx, user, "")
	if err != nil {
		log.Println(err)
		return
	}
	setCurrentUser(info)
	if info.Repo != nil {
		showRepo(info)
	} else {
		ListRepos(false)
		fmt.Println(warpRed("请设置 REPO ..."))
	}
}

// Deluser user account
func Deluser(user string) {
	err := buildLogCtlInfo()
	if err != nil {
		log.Printf("获取用户信息失败或无此用户: %s", user)
		return
	}
	if _logCtlCtx.Current == user {
		_logCtlCtx.Current = ""
	}
	if (*_logCtlCtx.Data)[user] != nil {
		(*_logCtlCtx.Data)[user] = nil
		delete((*_logCtlCtx.Data), user)
	}
	storeCtx(_logCtlCtx)
}

// UserList list user names
func UserList() {
	buildLogCtlInfo()
	users := make([]string, 0)
	if _logCtlCtx == nil || _logCtlCtx.Data == nil {
		fmt.Println(" 未找到已设置的登录信息")
		return
	}
	for user := range *_logCtlCtx.Data {
		users = append(users, user)
	}
	currentUser := ""
	if _info != nil {
		currentUser = _info.User
	}
	sort.Strings(users)
	for i, k := range users {
		if currentUser == k && len(currentUser) > 0 {
			fmt.Printf(warpRed("%v: %v %v\n"), i, k, "**")
		} else {
			fmt.Printf("%v: %v\n", i, k)
		}
	}
}

func buildUserContect(ak string, sk string, alias string) *logCtlInfo {
	buildLogCtlInfo()
	if _info != nil && _info.User == alias && _info.Ak == ak && _info.Sk == sk {
		return _info
	}
	info := &logCtlInfo{}
	info.User = alias
	info.Ak = ak
	info.Sk = sk
	return info
}

func setCurrentUser(info *logCtlInfo) (err error) {
	if _logCtlCtx == nil {
		_logCtlCtx = &logCtlCtx{}
	}
	_logCtlCtx.Current = info.User
	err = storeInfo(info)
	_info = nil
	return
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

type logCtlCtx struct {
	Data    *map[string]*logCtlCtxData `json:"data"`
	Current string                     `json:"current"`
}

type logCtlCtxData struct {
	AK    string                         `json:"ak"`
	SK    string                         `json:"sk"`
	Repo  string                         `json:"repo"`
	Range int                            `json:"range"`
	Data  *map[string]*logCtlCtxRepoData `json:"data"`
}

type logCtlCtxRepoData struct {
	Repo *logdb.GetRepoOutput    `json:"repo"`
	Log  *map[string]interface{} `json:"log"`
}

func buildLogCtlInfo() (err error) {
	if _info == nil {
		bytes, err := ioutil.ReadFile(userHomeDir() + "/" + _tempFile)
		if err != nil {
			return fmt.Errorf("%v\n 内部错误或还没有设置过账号信息", err)
		}
		if _logCtlCtx == nil {
			_logCtlCtx = &logCtlCtx{}
		}
		err = json.Unmarshal(bytes, _logCtlCtx)
		if err != nil {
			return fmt.Errorf("%v\n 内部错误或还没有设置过账号信息", err)
		}
		_info, err = getCtlInfo(_logCtlCtx, _logCtlCtx.Current, "")
		return err
	}
	return nil
}

func getCtlInfo(ctx *logCtlCtx, user string, repoName string) (*logCtlInfo, error) {
	if ctx.Data == nil {
		return nil, fmt.Errorf("获取用户信息失败或无此用户: %s ，请确认是否设置了正确的账号", user)
	}
	userData := (*ctx.Data)[user]
	if userData == nil {
		return nil, fmt.Errorf("获取用户信息失败或无此用户: %s ，请确认是否设置了正确的账号", user)
	}

	info := &logCtlInfo{}

	info.User = user
	info.Ak, _ = crypto.Decrypt(userData.AK)
	info.Sk, _ = crypto.Decrypt(userData.SK)
	info.Range = userData.Range
	if info.Range < 1 {
		info.Range = 5
	}
	if len(repoName) != 0 {
		info.RepoName = repoName
	} else {
		info.RepoName = userData.Repo
	}
	repoData := (*userData.Data)[info.RepoName]
	if repoData == nil {
		return info, nil
	}
	info.Repo = repoData.Repo
	info.Log = repoData.Log
	return info, nil
}

func storeInfo(v *logCtlInfo) (err error) {
	if v == nil {
		return fmt.Errorf("待保存的参数 *logCtlInfo 为 nil")
	}
	ctxDataMap := _logCtlCtx.Data
	if ctxDataMap == nil {
		temp := make(map[string]*logCtlCtxData)
		ctxDataMap = &temp
	}

	ctxData := (*ctxDataMap)[v.User]
	if ctxData == nil {
		ctxData = &logCtlCtxData{}
	}
	ctxData.AK, _ = crypto.Encrypt(v.Ak)
	ctxData.SK, _ = crypto.Encrypt(v.Sk)
	ctxData.Range = v.Range
	ctxData.Repo = v.RepoName

	ctxDataDataMap := ctxData.Data
	if ctxDataDataMap == nil {
		temp := make(map[string]*logCtlCtxRepoData)
		ctxDataDataMap = &temp
	}

	repoData := &logCtlCtxRepoData{}
	repoData.Repo = v.Repo
	repoData.Log = v.Log

	(*ctxDataDataMap)[v.RepoName] = repoData
	ctxData.Data = ctxDataDataMap

	(*ctxDataMap)[v.User] = ctxData
	_logCtlCtx.Data = ctxDataMap

	storeCtx(_logCtlCtx)
	return
}

func storeCtx(ctx *logCtlCtx) (err error) {
	bytes, err := json.Marshal(ctx)
	if err == nil && bytes != nil {
		err = ioutil.WriteFile(userHomeDir()+"/"+_tempFile, bytes, 0666)
		if err != nil {
			log.Println("Opps....", err)
		}
	}
	return
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
