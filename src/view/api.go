package view

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	base "qiniu.com/pandora/base"
	logdb "qiniu.com/pandora/logdb"
)

// CtlArg args
type CtlArg struct {
	Fields string
	Split  string
	Repo   string
	Sort   string
	Start  string
	End    string
	fields []string
	Head   int
	Debug  bool
}

// Query by query and args
func Query(query *string, arg *CtlArg) {
	info, _ := loadInfo()

	doQuery(query, arg, info, 0)
}

func doQuery(query *string, arg *CtlArg, info *logCtlInfo, from int) {
	size := 200
	if (arg.Head > 0) && (arg.Head < size) {
		size = arg.Head
	}

	if len(info.RepoName) == 0 {
		fmt.Println(" == set repo first. ==")
	}

	var dateField string
	for _, e := range info.Repo.Schema {
		if e.ValueType == "date" {
			dateField = e.Key
			break
		}
	}

	if (from == 0) && (len(dateField) != 0) {
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
		*query += " AND " + dateField + ": [" + start + " TO " + end + "]"
	}

	sort := arg.Sort
	if len(sort) == 0 && len(dateField) > 0 {
		sort = dateField + ":desc"
	}
	queryInput := &logdb.QueryLogInput{
		RepoName: info.RepoName,
		Query:    *query, //query字段sdk会自动做url编码，用户不需要关心
		Sort:     sort,
		From:     from,
		Size:     size,
	}

	if arg.Debug {
		log.Println(queryInput)
		log.Println()
	}

	client, err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	logs, err := client.QueryLog(queryInput)
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
			doQuery(query, arg, info, arg.Head)
		} else {
			doQuery(query, arg, info, from+size)
		}
	}
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
	for _, v := range strings.Split(arg.Fields, ",") {
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
	info, _ := loadInfo()
	if len(repoName) != 0 {
		client, err := buildClient()
		if err == nil {
			repo, err := client.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
			if err == nil {
				info.RepoName = repoName
				info.Repo = *repo
				storeInfo(info)
			}
		}
		if err != nil {
			fmt.Printf("Set repo: %s failed.\n", repoName)
		}
	}
	showRepo(&info.RepoName, &info.Repo)
}

// SetTimeRange set range
func SetTimeRange(r int) {
	info, _ := loadInfo()
	info.Range = r
	storeInfo(info)
}

// ListRepos list repos
func ListRepos(verbose bool) {
	client, err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	repos, err := client.ListRepos(&logdb.ListReposInput{}) // 列举repo
	if err != nil {
		log.Println(err)
		return
	}

	info, _ := loadInfo()

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
			if info.RepoName == v.RepoName {
				fmt.Printf(red("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s **")+"\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			} else {
				fmt.Printf("%3d:  %-"+sLen+"s\t%s\t%s\t%s\t%s\n",
					i, v.RepoName, v.Region, v.Retention, v.CreateTime, v.UpdateTime)
			}
		} else {
			if info.RepoName == v.RepoName {
				fmt.Printf(red("%3d:  %-"+sLen+"s\t%s\t%s **")+"\n",
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
	if len(repoName) == 0 {
		info, _ := loadInfo()
		showRepo(&info.RepoName, &info.Repo)
	} else {
		client, err := buildClient()
		if err != nil {
			log.Println(err)
			return
		}
		repo, err := client.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
		if err != nil {
			log.Println(err)
			return
		}
		showRepo(&repoName, repo)
	}
}

func showRepo(repoName *string, repo *logdb.GetRepoOutput) {
	fmt.Printf("%11s: %s\n", "RepoName", *repoName)
	fmt.Printf("%11s: %s\n", "Region", repo.Region)
	fmt.Printf("%11s: %s\n", "Retention", repo.Retention)
	fmt.Printf("%11s: %s\n", "CreateTime", repo.CreateTime)
	fmt.Printf("%11s: %s\n", "UpdateTime", repo.UpdateTime)
	fmt.Printf("Field: (%d)\n", len(repo.Schema))
	var dateField string
	for i, v := range repo.Schema {
		fmt.Printf("%3d: %s\n", i, v)
		if v.ValueType == "date" && len(dateField) == 0 {
			dateField = v.Key
		}
	}
	fmt.Printf("时间字段为： %s ，默认排序为： %s\n", red(dateField), red(dateField+":desc"))
}

func red(s string) string {
	return fmt.Sprintf("\033[0;31m%s\033[0m", s)
}

var tempFile = ".qn_logdb_ctl_profile"

type logCtlInfo struct {
	Range    int                 `json:"timeRange"`
	RepoName string              `json:"repoName"`
	Ak       string              `json:"ak"`
	Sk       string              `json:"sk"`
	Repo     logdb.GetRepoOutput `json:"repo"`
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

func loadInfo() (v *logCtlInfo, err error) {
	data, err := ioutil.ReadFile(userHomeDir() + "/" + tempFile)
	v = &logCtlInfo{}
	if err != nil {
		return
	}
	json.Unmarshal(data, v)
	return
}

func storeInfo(v *logCtlInfo) {
	bytes, err := json.Marshal(v)
	if err == nil && bytes != nil {
		err = ioutil.WriteFile(userHomeDir()+"/"+tempFile, bytes, 0666)
		log.Println("vvv", err)
	}
}

func buildClient() (logdb.LogdbAPI, error) {
	info, _ := loadInfo()
	cfg := logdb.NewConfig().
		WithAccessKeySecretKey(info.Ak, info.Sk).
		WithEndpoint("https://logdb.qiniu.com").
		WithDialTimeout(30 * time.Second).
		WithResponseTimeout(120 * time.Second).
		WithLogger(base.NewDefaultLogger()).
		WithLoggerLevel(base.LogDebug)
	return logdb.New(cfg)
}
