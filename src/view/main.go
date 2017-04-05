package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"

	base "qiniu.com/pandora/base"
	logdb "qiniu.com/pandora/logdb"
)

var tempFile = ".qiniu_bana_ctl_profile"

type LogCtlInfo struct {
	LineLimit int    `json:"lineLimit"`
	TimeRange int    `json:"timeRange"`
	Repo      string `json:"repo"`
	Ak        string `json:"ak"`
	Sk        string `json:"sk"`
}

func UserHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func loadInfo() (v *LogCtlInfo, err error) {
	data, err := ioutil.ReadFile(UserHomeDir() + "/" + tempFile)
	v = &LogCtlInfo{}
	if err != nil {
		return
	}
	json.Unmarshal(data, v)
	return
}

func storeInfo(v *LogCtlInfo) {
	bytes, err := json.Marshal(v)
	if err == nil && bytes != nil {
		err = ioutil.WriteFile(UserHomeDir()+"/"+tempFile, bytes, 0666)
		log.Println("vvv", err)
	}
}

func login(ak string, sk string) {
	v, _ := loadInfo()
	v.Ak = ak
	v.Sk = sk
	storeInfo(v)
}

func main() {
	var from int
	flag.IntVar(&from, "from", 0, "一次查询的起始位置，默认 0")
	var size int
	flag.IntVar(&size, "size", 200, "一次查询的行数，直接提供的值优先级高于 setLineLimit 设置的值")
	var sort string
	flag.StringVar(&sort, "sort", "", "如 timestarmp:desc ，默认不排序")
	var repo string
	flag.StringVar(&repo, "repo", "", "查询的 Repo，直接提供的值优先级高于 setRepo 设置的值")

	help1 := flag.Bool("help", false, "")
	help2 := flag.Bool("h", false, "")

	flag.Parse()

	help := *help1 || *help2

	exec := flag.Arg(0)

	if "help" == exec {
		showHelp()
	} else {
		v, _ := loadInfo()

		switch exec {
		case "login":
			if help {
				fmt.Println("qbana login <ak> <sk>")
			} else {
				login(flag.Arg(1), flag.Arg(2))
			}
			break
		case "setRepo":
			if help {
				fmt.Println("qbana setRepo <repoName>")
			} else {
				setRepo(flag.Arg(1))
			}
			break
		case "setLineLimit":
			if help {
				fmt.Println("qbana setLineLimit <lineLimit>")
			} else {
				i, err := strconv.Atoi(flag.Arg(1))
				if err != nil || i < 1 {
					log.Println("每次查询的行数限制，请提供大于 0 的整数")
				}
				setLineLimit(i)
			}
			break
		case "setTimeRange":
			if help {
				fmt.Println("qbana setTimeRange <timeRange>")
			} else {
				i, err := strconv.Atoi(flag.Arg(1))
				if err != nil || i < 1 {
					log.Println("每次查询的时间范围，请提供大于 0 的整数。单位 分钟")
				}
				setTimeRange(i)
			}
			break
		case "list":
			if help {
				fmt.Println("qbana list ")
			} else {
				listRepos()
			}
			break
		case "showRepo":
			if help {
				fmt.Println("qbana showRepo ")
				fmt.Println("qbana showRepo <repoName>")
			} else {
				repo = flag.Arg(1)
				if len(repo) < 1 {
					repo = v.Repo
				}
				showRepo(repo)
			}
			break
		case "query":
			if help {
				fmt.Println("qbana query <query> ")
				fmt.Println("qbana -repo <repoName> -sort timestamp:desc -size 500 query <query>")
			} else {
				if len(repo) < 1 {
					repo = v.Repo
				}
				if size < 1 {
					size = v.LineLimit
				}
				query(repo, flag.Arg(1), sort, from, size)
			}
			break
		default:
			showHelp()
		}
	}
}

func showHelp() {
	fmt.Println("qbana help")
	fmt.Println("qbana login <ak> <sk>")
	fmt.Println("qbana setRepo <repoName>")
	fmt.Println("qbana setLineLimit <lineLimit>")
	fmt.Println("qbana list ")
	fmt.Println("qbana showRepo ")
	fmt.Println("qbana showRepo <repoName>")
	fmt.Println("qbana query <query> ")
	fmt.Println("qbana -repo <repoName> -sort timestamp:desc -size 500 query <query>")
}

func buildClient() (logdb.LogdbAPI, error) {
	v, _ := loadInfo()
	cfg := logdb.NewConfig().
		WithAccessKeySecretKey(v.Ak, v.Sk).
		WithEndpoint("https://logdb.qiniu.com").
		WithLogger(base.NewDefaultLogger()).
		WithLoggerLevel(base.LogDebug)
	return logdb.New(cfg)
}

func listRepos() {
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
	for i, v := range repos.Repos {
		if info.Repo == v.RepoName {
			fmt.Printf("%3d: **** %s ****\n", i, v)
		} else {
			fmt.Printf("%3d:  %s\n", i, v)
		}
	}
}

func showRepo(repoName string) {
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
	fmt.Printf("%11s: %s\n", "RepoName", repoName)
	fmt.Printf("%11s: %s\n", "Region", repo.Region)
	fmt.Printf("%11s: %s\n", "Retention", repo.Retention)
	fmt.Printf("%11s: %s\n", "CreateTime", repo.CreateTime)
	fmt.Printf("%11s: %s\n", "UpdateTime", repo.UpdateTime)
	fmt.Printf("Field: (%d)\n", len(repo.Schema))
	for i, v := range repo.Schema {
		fmt.Printf("%3d: %s\n", i, v)
	}
}

func setRepo(repoName string) {
	v, _ := loadInfo()
	v.Repo = repoName
	storeInfo(v)
}

// 分钟
func setTimeRange(r int) {
	v, _ := loadInfo()
	v.TimeRange = r
	storeInfo(v)
}

func setLineLimit(l int) {
	v, _ := loadInfo()
	v.LineLimit = l
	storeInfo(v)
}

func query(repoName string, query string, sort string, from int, size int) {
	client, err := buildClient()
	if err != nil {
		log.Println(err)
		return
	}
	queryInput := &logdb.QueryLogInput{
		RepoName: repoName,
		Query:    query, //query字段sdk会自动做url编码，用户不需要关心
		Sort:     sort,
		From:     from,
		Size:     size,
		Highlight: &logdb.Highlight{
			PreTags:  []string{"p_Highlight_PreTags"},
			PostTags: []string{"p_Highlight_PostTags"},
		},
	}

	// PreTags:  []string{"\033[0;31m"},
	// PostTags: []string{"\033[0m"},

	logs, err := client.QueryLog(queryInput)
	if err != nil {
		log.Println(err)
		return
	}
	for i, v := range logs.Data {
		fmt.Printf("%3d: %s\n", i, v)
	}
}

func next() {

}
