package main

import (
	"errors"
	"fmt"
	"github.com/qiniuts/logctl/api"
	"gopkg.in/urfave/cli.v2"
	"os"
	"strconv"
	"time"
)

func normalizeDate(str string) (string, error) {
	// 没有指定时区，格式化为 0800
	dfs := []string{"20060102T15:04"}
	for _, df := range dfs {
		t, err := time.Parse(df, str)
		if err == nil {
			return t.Format("2006-01-02T15:04:05+0800"), err
		}
	}
	// 指定了时区
	dfs = []string{"2006-01-02T15:04:05-0700"}
	for _, df := range dfs {
		t, err := time.Parse(df, str)
		if err == nil {
			return t.Format("2006-01-02T15:04:05-0700"), err
		}
	}
	return "", errors.New(fmt.Sprintf(" %s : %s ", "时间格式不正确", str))
}

func main() {
	app := &cli.App{
		Name:      "logctl",
		Usage:     "query logs from logdb",
		UsageText: " command [command options] [arguments...]",
		Version:   "0.0.3",
		Commands: []*cli.Command{
			{
				Name:  "login",
				Usage: "设置后续查询时需要的 ak sk",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "alias",
						Aliases: []string{"a"},
						Value:   "default",
						Usage:   "登录用户别名`NAME`",
					},
				},
				Action: func(c *cli.Context) error {
					api.Login(c.Args().Get(0), c.Args().Get(1), c.String("alias"))
					return nil
				},
			},
			{
				Name:  "userlist",
				Usage: "已设置账号列表",
				Action: func(c *cli.Context) error {
					api.UserList()
					return nil
				},
			},
			{
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
				Action: func(c *cli.Context) error {
					api.ListRepos(c.Bool("v"))
					return nil
				},
			},
			{
				Name:  "repo",
				Usage: "设置查询日志所在的仓库(请在查询前设置)",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "refresh",
						Aliases: []string{"r"},
						Value:   false,
					},
				},
				Action: func(c *cli.Context) error {
					api.SetRepo(c.Args().Get(0), c.Bool("refresh"))
					return nil
				},
			},
			{
				Name:    "sample",
				Aliases: []string{"s"},
				Usage:   "显示两条日志作为样例",
				Action: func(c *cli.Context) error {
					api.QuerySample()
					return nil
				},
			},
			{
				Name:  "clear",
				Usage: "清理缓存信息",
				Action: func(c *cli.Context) error {
					api.Clear()
					return nil
				},
			},
			{
				Name:      "reqid",
				Usage:     "通过 reqid 查询日志。若未提供 [field:]，查看是否有 reqid、respheader 字段",
				ArgsUsage: " [<field>:]<reqid> ",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "field",
						Aliases: []string{"f"},
						Value:   "*",
						Usage:   "显示哪些字段，默认 * ，即全部。以逗号 , 分割，忽略空格。如 \"*, F1\"",
					},
					&cli.StringFlag{
						Name:  "split",
						Value: "\t",
						Usage: "显示字段分隔符",
					},
					&cli.BoolFlag{
						Name: "debug",
					},
				},
				Action: func(c *cli.Context) error {
					arg := &api.CtlArg{
						Field: c.String("field"),
						Split: c.String("split"),
						Debug: c.Bool("debug"),
					}
					api.QueryReqid(arg, c.Args().Get(0))
					return nil
				},
			},
			{
				Name:      "query",
				Aliases:   []string{"q"},
				Usage:     "在时间范围内查询 logdb 内的日志",
				ArgsUsage: " <query> ",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "order",
						Aliases: []string{"o"},
						Usage:   "排序字段. 必须包含 :desc 或 :asc， 如 timestamp:desc 。默认按 date 数据类型降序排列",
					},
					&cli.StringFlag{
						Name:    "start",
						Aliases: []string{"s"},
						Usage:   "查询日志的开始时间，格式要求 logdb 能够正确识别，如: 20060102T15:04，2017-04-06T17:40:30+0800",
					},
					&cli.StringFlag{
						Name:    "end",
						Aliases: []string{"e"},
						Usage:   "查询日志的终止时间，格式要求 logdb 能够正确识别，如: 20060102T15:04，2017-04-06T16:40:30+0800",
					},
					&cli.Float64Flag{
						Name:        "day",
						Aliases:     []string{"d"},
						Usage:       "从当前时间往前推指定天，如 2.5",
						DefaultText: "无",
					},
					&cli.Float64Flag{
						Name:        "hour",
						Aliases:     []string{"H"},
						Usage:       "从当前时间往前推指定小时，如 2.5",
						DefaultText: "无",
					},
					&cli.Float64Flag{
						Name:        "minute",
						Aliases:     []string{"m"},
						Usage:       "从当前时间往前推指定分钟，如 30",
						DefaultText: "无",
					},
					&cli.StringFlag{
						Name:    "field",
						Aliases: []string{"f"},
						Value:   "*",
						Usage:   "显示哪些字段，默认 * ，即全部。以逗号 , 分割，忽略空格。如 \"*, F1\"",
					},
					&cli.StringFlag{
						Name:  "split",
						Value: "\t",
						Usage: "显示字段分隔符",
					},
					&cli.IntFlag{
						Name:        "head",
						Aliases:     []string{"l"},
						Usage:       "显示前多少行",
						DefaultText: "无",
					},
					&cli.BoolFlag{
						Name:  "debug",
						Value: false,
						Usage: "显示参数信息",
					},
				},
				Action: func(c *cli.Context) (err error) {
					start := c.String("start")
					end := c.String("end")
					if len(start) != 0 {
						start, err = normalizeDate(start)
						if err != nil {
							fmt.Println(err)
							return nil
						}
					}
					if len(end) != 0 {
						end, err = normalizeDate(end)
						if err != nil {
							fmt.Println(err)
							return nil
						}
					}
					if (len(start) == 0) && (len(end) == 0) {
						day := c.Float64("day")
						hour := c.Float64("hour")
						minute := c.Float64("minute")

						m := day*24*60 + hour*60 + minute
						// 浮点数，不能通过 m != 0 判断
						if m > 0.05 {
							start = time.Now().Add(-time.Duration(m) * time.Minute).Format("2006-01-02T15:04:05-0700")
							end = time.Now().Format("2006-01-02T15:04:05-0700")
						}
					}
					arg := &api.CtlArg{
						Field: c.String("field"),
						Sort:  c.String("order"),
						Start: start,
						End:   end,
						Split: c.String("split"),
						Head:  c.Int("head"),
						Debug: c.Bool("debug"),
					}
					query := c.Args().Get(0)
					api.Query(&query, arg)
					return nil
				},
			},
			{
				Name:      "histogram",
				Aliases:   []string{"g"},
				Usage:     "在时间范围内查询 logdb 内的日志",
				Hidden:    true,
				ArgsUsage: " <query> ",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "start",
						Aliases: []string{"s"},
						Usage:   "查询日志的开始时间，如: 2017-04-06T17:40:30+0800",
					},
					&cli.StringFlag{
						Name:    "end",
						Aliases: []string{"e"},
						Usage:   "查询日志的终止时间，如: 2017-04-06T16:40:30+0800",
					},
					&cli.StringFlag{
						Name:    "field",
						Aliases: []string{"f"},
						Usage:   "以哪个字段排序，要求字段的数据类型为 date",
					},
					&cli.BoolFlag{
						Name:  "debug",
						Value: false,
						Usage: "显示参数信息",
					},
				},
				Action: func(c *cli.Context) error {
					arg := &api.CtlArg{
						Field: c.String("field"),
						Start: c.String("start"),
						End:   c.String("end"),
						Debug: c.Bool("debug"),
					}
					query := c.Args().Get(0)
					api.QueryHistogram(&query, arg)
					return nil
				},
			},
			{
				Name:  "range",
				Usage: "设置默认查询时间范围，单位 分钟，默认 5 分钟",
				Action: func(c *cli.Context) error {
					i, err := strconv.Atoi(c.Args().Get(0))
					if err == nil && i > 0 {
						api.SetTimeRange(i)
					} else {
						fmt.Println(" range must be an integer and greater than 0 ")
					}
					return nil
				},
			},
		},
	}

	app.Run(os.Args)
}
