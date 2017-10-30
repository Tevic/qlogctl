package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/qiniuts/qlogctl/api"
	"github.com/qiniuts/qlogctl/util"
	cli "gopkg.in/urfave/cli.v2"
)

var (
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

	split = &cli.StringFlag{
		Name:  "split",
		Value: "\t",
		Usage: "显示字段分隔符",
	}

	QueryByReqid = &cli.Command{
		Name:      "reqid",
		Usage:     "通过 reqid 查询日志。",
		UsageText: "查询条件为 reqid ，解析 reqid 设置时间范围。若未提供查询字段 [--reqidField <reqidField>]，则查看 repo 是否有 reqid、resppreSizeer 字段",
		// ArgsUsage: " [<field>:]<reqid> ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "reqidField",
				Usage: "指定从包含 reqid 的字段名",
			},
			dateFieldFlag, orderFlag, showfieldsFlag, split,
		},
		Action: func(c *cli.Context) error {
			arg := &api.CtlArg{
				Fields:    c.String("showfields"),
				Sort:      c.String("order"),
				DateField: c.String("dateField"),
				Split:     c.String("split"),
				PreSize:   c.Int("preSize"),
				Debug:     c.Bool("debug"),
			}
			err := api.QueryReqid(c.Args().Get(0), c.String("reqidField"), arg)
			return err
		},
	}

	Query = &cli.Command{
		Name:      "query",
		Aliases:   []string{"q"},
		Usage:     "在时间范围内查询 logdb 内的日志",
		ArgsUsage: " <query> ",
		Flags: []cli.Flag{

			&cli.StringFlag{
				Name:    "start",
				Aliases: []string{"s"},
				Usage:   "查询日志的开始时间，格式要求 logdb 能够正确识别，如: 20060102T15:04，2017-04-06T17:40:30+0800",
			},
			&cli.StringFlag{
				Name:    "end",
				Aliases: []string{"e"},
				Usage:   "查询日志的终止时间，格式要求 logdb 能够正确识别。如: 20060102T15:04，2017-04-06T16:40:30+0800",
			},
			&cli.Float64Flag{
				Name:        "day",
				Aliases:     []string{"d"},
				Usage:       "从当前时间往前推指定天，如 2.5。 day hour minute 可同时提供，",
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
			dateFieldFlag, orderFlag, showfieldsFlag, split,
			&cli.IntFlag{
				Name:        "preSize",
				Aliases:     []string{"l"},
				Usage:       "查询多少条数据，默认 100；有 --scroll 标记时，表示初次查询的条数，默认 2000。最大值 10000 ",
				DefaultText: "无",
			},
			&cli.BoolFlag{
				Name:  "scroll",
				Usage: "标记为 scroll 方式拉取日志。用于获取大量数据",
			},
		},
		Action: func(c *cli.Context) (err error) {
			var startDate time.Time
			var endDate time.Time
			start := c.String("start")
			end := c.String("end")
			if len(start) != 0 {
				startDate, err = normalizeDate(start)
				if err != nil {
					fmt.Println(err)
					return nil
				}
			}
			if len(end) != 0 {
				endDate, err = normalizeDate(end)
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
				Split:     c.String("split"),
				Scroll:    c.Bool("scroll"),
				Debug:     c.Bool("debug"),
			}

			if c.Int("preSize") < 1 {
				if c.Bool("scroll") {
					arg.PreSize = 2000
				} else {
					arg.PreSize = 100
				}
			} else {
				arg.PreSize = util.MinInt(c.Int("preSize"), 10000)
			}

			query := strings.Join(c.Args().Slice(), " ")
			err = api.Query(query, arg)
			return err
		},
	}
)
