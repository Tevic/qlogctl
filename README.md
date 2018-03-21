# qlogctl

qlogctl工具是针对Pandora日志检索产品提供的命令行工具，可以快速使用命令行查询日志检索中的数据。

## 源码安装
```
go get gopkg.in/urfave/cli.v2
go get github.com/qiniu/pandora-go-sdk
```

或
```
glide install
```

## 编译
```
go build -o qlogctl
```

## 帮助
```
qlogctl help
```

## 配置文件说明
配置文件要求 json 格式，允许额外的单行注释，注释内容为：以不在引号内的`#`开始，直到行结尾，`"#d"`不是注释。

ak、sk 为字符串；

repo 要求为包含字符串的数组；

其它字段会被忽略。
```
{
    "ak"="<My AccessKey>",
    "sk"="<My SecretKey>",
    "repo"=["<My RepoName1>", "<My RepoName1>"]
    "range"=5
}
```
也支持如下格式:
```
{
    # 账号 A
    "ak"="<My AccessKey>",
    "sk"="<My SecretKey>",
    "repo"=["<My RepoName1>", "<My RepoName1>"]

    # 账号 B
    # "ak"="<My AccessKey>",
    #"sk"="<My SecretKey>",
    #"repo"=["<My RepoName1>", "<My RepoName1>"],
    # "range"=5 # 若为指定查询范围，默认为最近 5 分钟内的日志
}
```

命令行中也可指定上面参数，若指定，以命令行参数为准，未指定，则配置文件中需要指定。


## 下载

 * [darwin 版本](http://devtools.qiniu.com/darwin/log/qlogctl_0.1.0)

 * [linux 版本](http://devtools.qiniu.com/linux/log/qlogctl_0.1.0)
