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
	debugFlag = &cli.BoolFlag{
		Name:   "debug",
		Hidden: true,
	}

	debugFlag2 = &cli.BoolFlag{
		Name:   "debug",
		Hidden: true,
	}

	configFlag = &cli.StringFlag{
		Name:    "config",
		Aliases: []string{"c"},
		Usage:   "config file",
	}

	akFlag = &cli.StringFlag{
		Name:  "ak",
		Usage: "设置 ak ，即 AccessKey ；优先级高于配置文件内容",
	}

	skFlag = &cli.StringFlag{
		Name:  "sk",
		Usage: "设置 sk ，即 SecretKey ；优先级高于配置文件内容",
	}

	repoFlag = &cli.StringFlag{
		Name:  "repo",
		Usage: "设置 repo，即 logdb 的名称 ；优先级高于配置文件内容",
	}

	dateFieldFlag = &cli.StringFlag{
		Name:  "dateField",
		Usage: "时间范围所作用的字段，如 timestamp 。若不指定，将自动寻找 repo 中类型为 date 的字段，没找到则忽略时间设置",
	}

	orderFieldFlag = &cli.StringFlag{
		Name:  "orderField",
		Usage: "排序的字段。若不指定，则查找类型为 date 的字段，没找到则不排序",
	}

	orderTypeFlag = &cli.StringFlag{
		Name:  "order",
		Value: "desc",
		Usage: "排序方式降序 desc 或升序 asc 。按指定的字段排序",
	}

	sortFlag = &cli.StringFlag{
		Name:  "sort",
		Usage: "排序，field1:asc,field2:desc, …。field 是实际字段名，asc代表升序，desc 代表降序。用逗号进行分隔。若设置此参数，则忽略“orderField”“order”参数",
	}

	showfieldsFlag = &cli.StringFlag{
		Name:  "showfields",
		Value: "*",
		Usage: "显示哪些字段，默认 * ，即全部。以逗号 , 分割，忽略空格。如 \"time, *\"",
	}

	noIndexFlag = &cli.BoolFlag{
		Name:  "noIndex",
		Usage: "查询结果是否显示行号",
	}

	splitFlag = &cli.StringFlag{
		Name:  "split",
		Value: "\t",
		Usage: "显示字段分隔符",
	}

	configFlags  = []cli.Flag{debugFlag, configFlag, akFlag, skFlag, repoFlag}
	queryFlags   = []cli.Flag{dateFieldFlag, sortFlag, orderFieldFlag, orderTypeFlag}
	showLogFlags = []cli.Flag{showfieldsFlag, noIndexFlag, splitFlag}

	listRepo = &cli.Command{
		Name:      "list",
		Aliases:   []string{"l"},
		Usage:     "列取当前账号下所有的仓库",
		ArgsUsage: " ",
		Flags: append(configFlags,
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Value:   false,
				Usage:   "verbose",
			},
		),
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c, false)
			if err != nil {
				return
			}
			err = api.ListRepos(conf, c.Bool("verbose"))
			return
		},
	}

	querySample = &cli.Command{
		Name:    "sample",
		Aliases: []string{"s"},
		Usage:   "显示一条样例记录",
		Flags:   configFlags,
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c, true)
			if err != nil {
				return
			}
			err = api.QuerySample(conf)
			return
		},
	}

	query = &cli.Command{
		Name:      "query",
		Aliases:   []string{"q"},
		Usage:     "在时间范围内查询 logdb 内的日志",
		ArgsUsage: " <query> \n 如 id:'\"abc*\"', id:abcd.jpg",
		Flags: append(append(append(configFlags, queryFlags...),
			&cli.StringFlag{
				Name:    "where",
				Aliases: []string{"w"},
				Usage:   "查询条件。建议将内容写在单引号内。若不指定此参数，则使用 非指令标记 的所有内容作为查询条件",
			},
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
				Usage:       "从当前时间往前推指定天，即 24 小时，如 2.5。 day hour minute 可同时提供，",
				DefaultText: "",
			},
			&cli.Float64Flag{
				Name:        "hour",
				Aliases:     []string{"H"},
				Usage:       "从当前时间往前推指定小时，即 60 分，如 2.5",
				DefaultText: "",
			},
			&cli.Float64Flag{
				Name:        "minute",
				Aliases:     []string{"m"},
				Usage:       "从当前时间往前推指定分钟，如 30",
				DefaultText: "",
			}),
			showLogFlags...),
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c, true)
			if err != nil {
				return
			}

			arg := mergeArgFlag(c)
			start, end, err := mergeDateTimeFlag(c)
			if err != nil {
				return
			}
			arg.Start = start
			arg.End = end

			query := c.String("where")
			if strings.TrimSpace(query) == "" {
				query = strings.Join(c.Args().Slice(), " ")
			}
			err = api.Query(conf, query, arg)
			return
		},
	}

	queryByReqid = &cli.Command{
		Name:      "reqid",
		Usage:     "通过 reqid 查询日志。",
		UsageText: "查询条件为 reqid ，解析 reqid 设置时间范围: [field:] <reqidField>.若未提供查询字段 ，则查看 repo 是否有 reqid、resppreSizeer 字段",
		// ArgsUsage: " [<field>:]<reqid> ",
		Flags: append(append(append(configFlags, queryFlags...),
			&cli.StringFlag{
				Name:    "where",
				Aliases: []string{"w"},
				Usage:   "查询条件。[field:] <reqidField>。若不指定此参数，则使用 非指令标记 的所有内容作为查询条件",
			}),
			showLogFlags...),
		Action: func(c *cli.Context) (err error) {
			conf, err := loadConfigAndMergeFlag(c, true)
			if err != nil {
				return
			}
			arg := mergeArgFlag(c)
			w := c.String("where")
			if strings.TrimSpace(w) == "" {
				w = c.Args().Get(0)
			}
			w = strings.TrimSpace(w)
			ss := strings.Split(w, ":")
			reqid := ss[len(ss)-1]
			field := ""
			if len(ss) > 1 {
				field = strings.Join(ss[:len(ss)-1], ":")
			}
			if reqid == "" {
				err = errors.New("ERROR: HAVE NOT set repo ")
				return
			}
			err = api.QueryReqid(conf, reqid, field, arg)
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
		Commands: []*cli.Command{
			listRepo, querySample,
			query, queryByReqid,
		},
		EnableShellCompletion: true,
	}
	return &app
}

func loadConfigAndMergeFlag(c *cli.Context, needRepo bool) (*api.Config, error) {
	configFilePath := c.String("config")
	var conf = api.Config{}
	if configFilePath != "" {
		err := loadEx(&conf, &configFilePath)
		if err != nil {
			return nil, err
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
		err := errors.New("ERROR: HAVE NOT set ak and/or sk ")
		return nil, err
	}

	if needRepo && len(conf.Repo) == 0 {
		err := errors.New("ERROR: HAVE NOT set repo ")
		return nil, err
	}

	logLevel := log.Linfo
	if conf.Debug {
		logLevel = log.Ldebug
	}
	log.SetOutputLevel(logLevel)

	return &conf, nil
}

func mergeArgFlag(c *cli.Context) *api.CtlArg {
	arg := &api.CtlArg{
		Fields:     c.String("showfields"),
		DateField:  c.String("dateField"),
		Sort:       c.String("sort"),
		OrderField: c.String("orderField"),
		OrderType:  c.String("order"),
		ShowIndex:  !c.Bool("noIndex"),
		Split:      c.String("split"),
		PreSize:    c.Int("preSize"),
		Scroll:     c.Bool("scroll"),
	}
	if len(arg.Fields) == 0 {
		arg.Fields = "*"
	}
	if arg.OrderType != "asc" {
		arg.OrderType = "desc"
	}
	if arg.PreSize < 1 {
		if arg.Scroll {
			arg.PreSize = 2000
		} else {
			arg.PreSize = 100
		}
	} else {
		arg.PreSize = api.MinInt(arg.PreSize, 10000)
	}
	return arg
}

func mergeDateTimeFlag(c *cli.Context) (startDate *time.Time,
	endDate *time.Time, err error) {
	startDate = &time.Time{}
	endDate = &time.Time{}

	start := c.String("start")
	end := c.String("end")
	if len(start) != 0 {
		*startDate, err = normalizeDate(start)
		if err != nil {
			return
		}
	}
	if len(end) != 0 {
		*endDate, err = normalizeDate(end)
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
			*startDate = time.Now().Add(-time.Duration(m) * time.Minute)
			*endDate = time.Now()
		}
	}
	if (*startDate).After(*endDate) {
		temp := startDate
		startDate = endDate
		endDate = temp
	}
	if endDate.IsZero() {
		*endDate = time.Now()
	}
	if startDate.IsZero() {
		*startDate = (*endDate).Add(-time.Minute * 5)
	}
	return
}
