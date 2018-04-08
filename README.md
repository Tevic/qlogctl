# qlogctl

qlogctl工具是针对Pandora日志检索产品提供的命令行工具，可以快速使用命令行查询日志检索中的数据。

## 源码安装
下载源码后
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

## 查询
ak sk repo 信息可以从配置文件中读取，也可在参数中指定。若都有设置，以参数中指定的为准。
```
qlogctl help query

qlogctl q -c customer-config.json --ak <ak> --repo repo_test --where 'respheader:"Android"'

qlogctl q -c customer-config.json --repo repo_test --where 'respheader:"Android"' --all


nohup qlogctl q -c customer-config.json --repo repo_test --all -w 'respheader:"Android"'  > some.log 2>err.log &
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
    "ak":"My AccessKey",
    "sk":"My SecretKey",
    "repo":["RepoName1"]
}
```
也支持如下格式:
```
{
    # 非引号内，以#号开始到行尾，为注释，会被忽略
    #"ak":"My AccessKey"
    #,"sk":"My SecretKey"
    #,"repo":["RepoName1"] # 当前只有第一个 repo 有效 

    # 账号 B
    "ak":"My AccessKey"
    ,"sk":"My SecretKey"
    ,"repo":["RepoName1"]
}
```

命令行中也可指定上面参数，若指定，以命令行参数为准，未指定，则配置文件中需要指定。


## 下载

 * [darwin 版本](http://devtools.qiniu.com/qlogctl_darwin_amd64_0.1.0?t=1522228068)

 * [linux 版本](http://devtools.qiniu.com/qlogctl_linux_amd64_0.1.0?t=1522228068)
