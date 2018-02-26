package cmd

import (
	"errors"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniuts/qlogctl/api"
	"gopkg.in/urfave/cli.v2"
)

var (
	listRepo = &cli.Command{
		Name:      "list",
		Aliases:   []string{"l"},
		Usage:     "列取当前账号下所有的仓库",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Value:   false,
				Usage:   "verbose",
			},
		},
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c)
			if err != nil {
				return
			}
			err = api.ListRepos(&conf, c.Bool("verbose"))
			return
		},
	}

	querySample = &cli.Command{
		Name:    "sample",
		Aliases: []string{"s"},
		Usage:   "显示一条样例记录",
		Flags: []cli.Flag{
			ak, sk, repo,
		},
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c)
			if err != nil {
				return
			}
			err = api.QuerySample(&conf)
			return
		},
	}

	ak = &cli.StringFlag{
		Name:  "ak",
		Usage: "设置 ak ，即 AccessKey ；优先级高于配置文件内容",
	}

	sk = &cli.StringFlag{
		Name:  "sk",
		Usage: "设置 sk ，即 SecretKey ；优先级高于配置文件内容",
	}

	repo = &cli.StringFlag{
		Name:  "repo",
		Usage: "设置 repo，即 logdb 的名称 ；优先级高于配置文件内容",
	}

	dateFieldFlag = &cli.StringFlag{
		Name:  "dateField",
		Usage: "时间范围所作用的字段，如 timestamp 。若未设置，将自动寻找 repo 中类型为 date 的字段",
	}

	orderFlag = &cli.StringFlag{
		Name:  "order",
		Value: "desc",
		Usage: "排序方式 desc 或 asc 。按时间字段排序",
	}

	showfieldsFlag = &cli.StringFlag{
		Name:  "showfields",
		Value: "*",
		Usage: "显示哪些字段，默认 * ，即全部。以逗号 , 分割，忽略空格。如 \"time, *\"",
	}

	noIndex = &cli.BoolFlag{
		Name:  "noIndex",
		Usage: "查询结果是否显示行号",
	}

	split = &cli.StringFlag{
		Name:  "split",
		Value: "\t",
		Usage: "显示字段分隔符",
	}

	query = &cli.Command{
		Name:      "query",
		Aliases:   []string{"q"},
		Usage:     "在时间范围内查询 logdb 内的日志",
		ArgsUsage: " <query> ",
		Flags: []cli.Flag{
			ak, sk, repo,
			&cli.BoolFlag{
				Name:    "scroll",
				Aliases: []string{"all"},
				Usage:   "标记为 scroll 方式拉取日志。加此参数，表示获取满足条件的所有数据",
			},
			&cli.IntFlag{
				Name:        "preSize",
				Aliases:     []string{"l"},
				Usage:       "查询数据条数，默认 100，最大值 10000；有 --scroll 标记时内部会多次拉取数据，表示“每次”拉取的条数，默认 2000 (获取满足条件的所有数据)。",
				DefaultText: " ",
			},
			dateFieldFlag,
			&cli.StringFlag{
				Name:    "start",
				Aliases: []string{"s"},
				Usage:   "查询日志的开始时间。如: 20060102T15:04，20060102T15:04:05，2017-04-06T17:40:30+0800",
			},
			&cli.StringFlag{
				Name:    "end",
				Aliases: []string{"e"},
				Usage:   "查询日志的终止时间。如: 20060102T15:04，20060102T15:04:05，2017-04-06T16:40:30+0800",
			},
			&cli.Float64Flag{
				Name:        "day",
				Aliases:     []string{"d"},
				Usage:       "从当前时间往前推指定天，如 2.5。 day hour minute 可同时提供，",
				DefaultText: "",
			},
			&cli.Float64Flag{
				Name:        "hour",
				Aliases:     []string{"H"},
				Usage:       "从当前时间往前推指定小时，如 2.5",
				DefaultText: "",
			},
			&cli.Float64Flag{
				Name:        "minute",
				Aliases:     []string{"m"},
				Usage:       "从当前时间往前推指定分钟，如 30",
				DefaultText: "",
			},
			orderFlag, showfieldsFlag, noIndex, split,
		},
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c)
			if err != nil {
				return
			}
			var startDate time.Time
			var endDate time.Time
			start := c.String("start")
			end := c.String("end")
			if len(start) != 0 {
				startDate, err = normalizeDate(start)
				if err != nil {
					return
				}
			}
			if len(end) != 0 {
				endDate, err = normalizeDate(end)
				if err != nil {
					return
				}
			}
			if (len(start) == 0) && (len(end) == 0) {
				day := c.Float64("day")
				hour := c.Float64("hour")
				minute := c.Float64("minute")

				m := day*24*60 + hour*60 + minute
				// 浮点数，不能通过 m != 0 判断
				if m > 0.05 {
					startDate = time.Now().Add(-time.Duration(m) * time.Minute)
					endDate = time.Now()
				}
			}

			arg := &api.CtlArg{
				Fields:    c.String("showfields"),
				Sort:      c.String("order"),
				DateField: c.String("dateField"),
				Start:     startDate,
				End:       endDate,
				ShowIndex: !c.Bool("noIndex"),
				Split:     c.String("split"),
				Scroll:    c.Bool("scroll"),
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
				arg.Start = arg.End.Add(-time.Minute * 5)
			}

			if err != nil {
				return
			}
			if len(arg.Fields) == 0 {
				arg.Fields = "*"
			}
			if len(arg.Sort) == 0 {
				arg.Sort = "desc"
			}
			if c.Int("preSize") < 1 {
				if c.Bool("scroll") {
					arg.PreSize = 2000
				} else {
					arg.PreSize = 100
				}
			} else {
				arg.PreSize = api.MinInt(c.Int("preSize"), 10000)
			}
			query := strings.Join(c.Args().Slice(), " ")
			err = api.Query(&conf, query, arg)
			return
		},
	}

	queryByReqid = &cli.Command{
		Name:      "reqid",
		Usage:     "通过 reqid 查询日志。",
		UsageText: "查询条件为 reqid ，解析 reqid 设置时间范围。若未提供查询字段 [--field <reqidField>]，则查看 repo 是否有 reqid、resppreSizeer 字段",
		// ArgsUsage: " [<field>:]<reqid> ",
		Flags: []cli.Flag{
			ak, sk, repo,
			&cli.StringFlag{
				Name:  "field",
				Usage: "指定包含 reqid 的字段名",
			},
			dateFieldFlag, orderFlag, showfieldsFlag, noIndex, split,
		},
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c)
			if err != nil {
				return
			}
			arg := &api.CtlArg{
				Fields:    c.String("showfields"),
				Sort:      c.String("order"),
				DateField: c.String("dateField"),
				ShowIndex: !c.Bool("noIndex"),
				Split:     c.String("split"),
				PreSize:   c.Int("preSize"),
			}
			err = api.QueryReqid(&conf, c.Args().Get(0), c.String("field"), arg)
			return
		},
	}
)

func BuildApp() *cli.App {
	app := cli.App{
		Name:      "qlogctl",
		Usage:     "query logs from logdb",
		UsageText: " command [command options] [arguments...] ",
		Version:   "0.1.0",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:   "debug",
				Hidden: true,
			},
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "config file",
			},
		},
		Commands: []*cli.Command{
			listRepo, querySample,
			query, queryByReqid,
		},
		EnableShellCompletion: true,
	}
	return &app
}

func loadConfigAndMergeFlag(c *cli.Context) (conf api.Config, err error) {
	configFilePath := c.String("config")
	if configFilePath != "" {
		err = loadEx(&conf, &configFilePath)
		if err != nil {
			return
		}
	}

	ak := c.String("ak")
	sk := c.String("sk")
	repo := c.String("repo")
	debug := c.Bool("debug")
	if ak != "" {
		conf.Ak = ak
	}
	if sk != "" {
		conf.Sk = sk
	}
	if repo != "" {
		conf.Repo = strings.Split(repo, ",")
	}
	if debug {
		conf.Debug = debug
	}

	// remove empty string
	vsf := make([]string, 0)
	for _, v := range conf.Repo {
		v = strings.TrimSpace(v)
		if v != "" {
			vsf = append(vsf, v)
		}
	}
	conf.Repo = vsf

	if conf.Ak == "" || conf.Sk == "" {
		err = errors.New("ERROR: HAVE NOT set ak and/or sk ")
		return
	}

	logLevel := log.Linfo
	if conf.Debug {
		logLevel = log.Ldebug
	}
	log.SetOutputLevel(logLevel)

	return
}
