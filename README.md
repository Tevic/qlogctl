# qlogctl

qlogctl工具是针对Pandora日志检索产品提供的命令行工具，可以快速使用命令行查询日志检索中的数据，也是日志检索的延伸，可以搜索更大数据量。

## 源码安装

```
go get gopkg.in/urfave/cli.v2
go get github.com/qiniu/pandora-go-sdk
go build -o qlogctl
```

## 注意

0.0.4 版本开始，对 ak sk 加密保存读取，老版本的请先 clear 后重新设置账号


## 帮助

```
qlogctl help
```


## 下载

 * [darwin 版本](http://devtools.qiniu.com/darwin/log/qlogctl_0.0.6)

 * [linux 版本](http://devtools.qiniu.com/linux/log/qlogctl_0.0.6)
