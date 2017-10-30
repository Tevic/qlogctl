package cmd

import (
	"gopkg.in/urfave/cli.v2"
)

var (
	QueryForHistogram = &cli.Command{
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
			// arg := &api.CtlArg{
			// 	Fields: c.String("field"),
			// 	Start:  c.String("start"),
			// 	End:    c.String("end"),
			// 	Debug:  c.Bool("debug"),
			// }
			// query := strings.Join(c.Args().Slice(), " ")
			// api.QueryHistogram(&query, arg)
			return nil
		},
	}
)
