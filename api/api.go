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

	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniuts/qlogctl/log"
	"github.com/qiniuts/qlogctl/util"
)

const (
	DateLayout = "2006-01-02T15:04:05-0700"
)

// CtlArg args
type CtlArg struct {
	Fields    string    // 显示展示哪些字段，* 表示全部字段。字段名以逗号 , 分割，忽略空格
	Split     string    // 显示时，各字段的分割方式
	DateField string    //时间范围所作用的字段，如 timestamp
	Sort      string    // 排序方式 desc 或 asc 。按时间字段排序
	Start     time.Time // 查询的起始时间
	End       time.Time // 查询的结束时间
	PreSize   int       // 每次查询多少条
	Scroll    bool      // 是否使用 scroll 方式拉取数据
	Debug     bool      // 是否打印调试信息。 true 表示打印
	fields    []logdb.RepoSchemaEntry
}

func checkCtlArg(arg *CtlArg, info *logCtlInfo) (warn, err error) {
	if len(currentInfo.RepoName) == 0 {
		err = fmt.Errorf("\n 请先设置要查询的 REPO \n")
		return
	}
	if (arg.Start).After(arg.End) {
		start := arg.Start
		arg.Start = arg.End
		arg.End = start
	}
	if arg.End.IsZero() {
		arg.End = time.Now()
	}
	if arg.Start.IsZero() {
		arg.Start = arg.End.Add(-time.Minute * time.Duration(currentInfo.Range))
	}
	warn, err = checkInRetention(&arg.Start, &arg.End, strings.ToLower(currentInfo.Repo.Retention))
	if err != nil {
		return
	}

	if len(arg.Fields) == 0 {
		arg.Fields = "*"
	}
	if len(arg.Sort) == 0 {
		arg.Sort = "desc"
	}
	if arg.PreSize < 1 {
		arg.PreSize = 1000
	}
	return
}

// retention :eg: 7d, 30d
func checkInRetention(start, end *time.Time, retention string) (warn, err error) {

	//forever storage
	if strings.TrimSpace(retention) == "-1" {
		return
	}

	day := 0
	for _, c := range retention {
		if unicode.IsDigit(c) {
			// ascii , 48 ==> 0
			day = day*10 + int(c-48)
		} else {
			break
		}
	}

	earliest := time.Now().Add(-time.Hour * 24 * time.Duration(day))
	if earliest.After(*end) {
		err = fmt.Errorf("[%v ~ %v]时间范围太过久远，要求在 \"%s\" 之内。", start, end, retention)
		return
	}

	if earliest.After(*start) {
		warn = fmt.Errorf("[%v ~ %v]时间可能超出范围，不一定能获取到有效数据。最好在 \"%s\" 之内。", start, end, retention)
	}
	return
}

// Query by query and args
func Query(query string, arg *CtlArg) (err error) {
	setDebug(arg.Debug)
	log.Debugf("query: %v\nargs: %+v\n", query, *arg)
	err = queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}
	warn, err := checkCtlArg(arg, currentInfo)
	if warn != nil {
		log.Infoln(warn)
	}
	if err != nil {
		log.Infoln(err)
		return
	}
	log.Debugf("query: %v\nargs: %+v\n", query, *arg)
	sort := buildQueryStr(&query, currentInfo, arg)

	log.Debugf("query: %v\nargs: %+v\n", query, *arg)
	err = execQuery(&query, arg, currentInfo, sort, arg.PreSize)
	return
}

func buildQueryStr(pquery *string, info *logCtlInfo, arg *CtlArg) (sort string) {
	dateField, sort := getDateFieldAndSort(info.Repo, &arg.DateField, &arg.Sort)
	log.Debugln(dateField, sort)
	if len(dateField) != 0 {
		query := *pquery
		if len(query) != 0 {
			query += " AND "
		}
		query += dateField + ":[" + arg.Start.Format(DateLayout) +
			" TO " + arg.End.Format(DateLayout) + "]"
		*pquery = query
	}
	return
}

func getDateFieldAndSort(repo *logdb.GetRepoOutput, dateField, order *string) (string, string) {
	log.Debugln(*dateField, len(*dateField))
	if len(*dateField) > 0 {
		return *dateField, *dateField + ":" + *order
	}
	for _, e := range repo.Schema {
		if e.ValueType == "date" {
			return e.Key, e.Key + ":" + *order
		}
	}
	return "", ""
}

func execQuery(query *string, arg *CtlArg, info *logCtlInfo, sort string, firstSize int) (err error) {
	logs, err := doQuery(query, info, sort, firstSize, arg.Scroll)
	if err != nil {
		log.Infoln(err)
		return
	}

	log.Debugf("FirstQuery: [scroll: %v...(%v), total:%v, state:%v, size: %v]\n", logs.ScrollId[:util.MinInt(23, len(logs.ScrollId))], len(logs.ScrollId), logs.Total, logs.PartialSuccess, len(logs.Data))

	size := len(logs.Data)
	total := size
	showLogs(logs, arg, info, 0)
	logs.Data = nil

	for logs.Total > total && len(logs.ScrollId) > 1 && size > 0 {
		scrollInput := &logdb.QueryScrollInput{
			RepoName: info.RepoName,
			ScrollId: logs.ScrollId,
			Scroll:   "2m",
		}
		logs, err = logdbClient.QueryScroll(scrollInput)
		if err != nil {
			log.Infoln(err)
			return
		}
		log.Debugf("scroll: %v, logstotal:%v, state:%v, size: %v, total: %v\n", logs.ScrollId, logs.Total, logs.PartialSuccess, size, total)
		showLogs(logs, arg, info, total)
		size = len(logs.Data)
		total += size
		logs.Data = nil
		err = nil
	}
	return
}

func doQuery(query *string, info *logCtlInfo, sort string, size int, srcoll bool) (logs *logdb.QueryLogOutput, err error) {
	queryInput := &logdb.QueryLogInput{
		RepoName: info.RepoName,
		Query:    *query, //query字段sdk会自动做url编码，用户不需要关心
		Sort:     sort,
		From:     0,
		Size:     size,
	}

	if srcoll {
		queryInput.Scroll = "3m"
	}

	log.Debugf("%+v\n", *queryInput)

	err = buildClient()
	if err != nil {
		log.Infoln(err)
		return
	}

	return logdbClient.QueryLog(queryInput)
}

func showLogs(logs *logdb.QueryLogOutput, arg *CtlArg, info *logCtlInfo, from int) {
	if arg.fields == nil || len(arg.fields) == 0 {
		arg.fields = getShowFields(arg.Fields, info.Repo)
	}

	for i, v := range logs.Data {
		fmt.Printf("%d\t%s\n", i+from, formatDbLog(&v, &arg.fields, arg.Split, false))
	}
}

func formatDbLog(log *map[string]interface{}, fields *[]logdb.RepoSchemaEntry, split string, verbose bool) string {
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

//QueryReqid query logs by reqid
func QueryReqid(reqid string, reqidField string, arg *CtlArg) (err error) {
	setDebug(arg.Debug)
	// 正确格式的 reqid
	unixNano, err := parseReqid(reqid)
	if err != nil {
		log.Infoln("reqid 格式不正确：", err)
		return
	}
	err = queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}

	// 构建查询语句，指定查询字段
	if len(reqidField) == 0 {
		reqidField = getReqidField(currentInfo, "reqid", "respheader")
	}

	if len(reqidField) == 0 {
		log.Infoln("没有找到合适的字段用于查询 reqid，请使用 --reqidField <reqidField> 指定字段")
		return
	}
	query := reqidField + ":" + reqid
	t := time.Unix(unixNano/1e9, 0)
	arg.Start = t.Add(-time.Minute * 3)
	arg.End = t.Add(time.Minute * 3)
	err = Query(query, arg)
	return
}

func getReqidField(info *logCtlInfo, fields ...string) string {
	for _, field := range fields {
		for _, e := range info.Repo.Schema {
			if strings.ToLower(e.Key) == field {
				return e.Key
			}
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
	err := queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}
	if !refresh {
		if len(repoName) == 0 && currentInfo.Repo == nil {
			fmt.Println("Repo 为空，请指定 reponame")
			return
		}
		if len(repoName) == 0 || repoName == currentInfo.RepoName {
			showRepo(currentInfo)
			return
		}
		info, err := getCtlInfo(currentLogCtlCtx, currentInfo.User, repoName)
		if err != nil {
			log.Infoln(err)
			return
		}
		if info != nil && info.Repo != nil {
			showRepo(info)
			storeInfo(info)
			return
		}
	}
	if len(repoName) != 0 {
		repo, err := getNewRepoInfoByName(repoName)
		if err != nil {
			log.Infoln(err)
			return
		}
		currentInfo.RepoName = repoName
		currentInfo.Repo = repo
	}
	sample, err := doQuerySample(currentInfo)
	if err == nil {
		currentInfo.Log = sample
	}
	storeInfo(currentInfo)
	showRepo(currentInfo)
}

// QuerySample sample
func QuerySample() {
	err := queryLogCtlInfo()
	if err != nil {
		fmt.Println(err)
		return
	}
	if currentInfo.Log == nil {
		sample, err := doQuerySample(currentInfo)
		if err != nil {
			log.Infoln(err)
			return
		}
		currentInfo.Log = sample
		storeInfo(currentInfo)
	}
	// 显示样例
	if currentInfo.Log != nil {
		fields := getShowFields("*", currentInfo.Repo)
		fmt.Printf("%s\n", formatDbLog(currentInfo.Log, &fields, "\n", true))
	}
}

// ListRepos list repos
func ListRepos(verbose bool) (err error) {
	err = queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}
	err = buildClient()
	if err != nil {
		log.Infoln(err)
		return
	}
	repos, err := logdbClient.ListRepos(&logdb.ListReposInput{}) // 列举repo
	if err != nil {
		log.Infoln(err)
		return
	}

	sort.Slice(repos.Repos, func(i, j int) bool {
		return repos.Repos[i].RepoName < repos.Repos[j].RepoName
	})

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
			if currentInfo.RepoName == v.RepoName {
				fmt.Printf(warpRed("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s **")+"\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			} else {
				fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			}
		} else {
			if currentInfo.RepoName == v.RepoName {
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

func getNewRepoInfoByName(repoName string) (repo *logdb.GetRepoOutput, err error) {
	err = buildClient()
	if err != nil {
		return
	}

	repo, err = logdbClient.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
	if err != nil {
		return
	}
	return
}

func doQuerySample(info *logCtlInfo) (log *map[string]interface{}, err error) {
	arg := &CtlArg{}
	checkCtlArg(arg, info)
	query := "*"
	logs, err := doQuery(&query, info, "", 2, false)

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
		fmt.Printf("%s\n", formatDbLog(info.Log, &fields, "\n", true))
	}
}

func warpRed(s string) string {
	return fmt.Sprintf("\033[0;31m%s\033[0m", s)
}

// Account the ak,sk and give them a name
func Account(ak string, sk string, name string) {
	info := buildUserContent(ak, sk, name)
	if info == currentInfo && info.Repo != nil {
		showRepo(info)
		return
	}
	if info != currentInfo {
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
	err := queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}
	info, err := getCtlInfo(currentLogCtlCtx, user, "")
	if err != nil {
		log.Infoln(err)
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
	err := queryLogCtlInfo()
	if err != nil {
		log.Errorf("获取用户信息失败或无此用户: %s", user)
		return
	}
	if currentLogCtlCtx.Current == user {
		currentLogCtlCtx.Current = ""
	}
	if (*currentLogCtlCtx.Data)[user] != nil {
		(*currentLogCtlCtx.Data)[user] = nil
		delete((*currentLogCtlCtx.Data), user)
	}
	storeCtx(currentLogCtlCtx)
}

// UserList list user names
func UserList() {
	queryLogCtlInfo()
	users := make([]string, 0)
	if currentLogCtlCtx == nil || currentLogCtlCtx.Data == nil {
		fmt.Println(" 未找到已设置的登录信息")
		return
	}
	for user := range *currentLogCtlCtx.Data {
		users = append(users, user)
	}
	currentUser := ""
	if currentInfo != nil {
		currentUser = currentInfo.User
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
