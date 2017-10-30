package cmd

import (
	"fmt"
	"strconv"

	"github.com/qiniuts/qlogctl/api"
	"gopkg.in/urfave/cli.v2"
)

var (
	ListRepo = &cli.Command{
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
	}

	SetRepo = &cli.Command{
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
	}

	QuerySample = &cli.Command{
		Name:  "sample",
		Usage: "显示日志作为样例",
		Action: func(c *cli.Context) error {
			api.QuerySample()
			return nil
		},
	}

	SetRange = &cli.Command{
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
	}
)
