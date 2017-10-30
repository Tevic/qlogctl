package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniuts/qlogctl/log"
)

// 用户，查看的 repo，及其样例信息
type logCtlInfo struct {
	User     string                  `json:"user"`
	Range    int                     `json:"timeRange"`
	RepoName string                  `json:"repoName"`
	Ak       string                  `json:"ak"`
	Sk       string                  `json:"sk"`
	Repo     *logdb.GetRepoOutput    `json:"repo"`
	Log      *map[string]interface{} `json:"log"`
}

var currentInfo *logCtlInfo
var currentLogCtlCtx *logCtlCtx
var logdbClient logdb.LogdbAPI

const _tempFile = ".qn_logdb_ctl_profile"

func setDebug(isDebug bool) {
	if isDebug {
		log.Logger.SetLevel(log.DEBUG)
	}
}

// Clear the cache
func Clear() {
	os.Remove(userHomeDir() + "/" + _tempFile)
}

// SetTimeRange set range
func SetTimeRange(r int) {
	err := queryLogCtlInfo()
	if err != nil {
		log.Infoln(err)
		return
	}
	currentInfo.Range = r
	storeInfo(currentInfo)
}

func buildUserContent(ak string, sk string, alias string) *logCtlInfo {
	queryLogCtlInfo()
	if currentInfo != nil && currentInfo.User == alias && currentInfo.Ak == ak && currentInfo.Sk == sk {
		return currentInfo
	}
	info := &logCtlInfo{}
	info.User = alias
	info.Ak = ak
	info.Sk = sk
	return info
}

func setCurrentUser(info *logCtlInfo) (err error) {
	if currentLogCtlCtx == nil {
		currentLogCtlCtx = &logCtlCtx{}
	}
	currentLogCtlCtx.Current = info.User
	err = storeInfo(info)
	currentInfo = nil
	return
}

func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

type logCtlCtx struct {
	Data    *map[string]*logCtlCtxData `json:"data"`
	Current string                     `json:"current"`
}

type logCtlCtxData struct {
	AK    string                         `json:"ak"`
	SK    string                         `json:"sk"`
	Repo  string                         `json:"repo"`
	Range int                            `json:"range"`
	Data  *map[string]*logCtlCtxRepoData `json:"data"`
}

type logCtlCtxRepoData struct {
	Repo *logdb.GetRepoOutput    `json:"repo"`
	Log  *map[string]interface{} `json:"log"`
}

func queryLogCtlInfo() (err error) {
	if currentInfo == nil {
		bytes, err := ioutil.ReadFile(userHomeDir() + "/" + _tempFile)
		if err != nil {
			return fmt.Errorf("%v\n 内部错误或还没有设置过账号信息", err)
		}
		if currentLogCtlCtx == nil {
			currentLogCtlCtx = &logCtlCtx{}
		}
		err = json.Unmarshal(bytes, currentLogCtlCtx)
		if err != nil {
			return fmt.Errorf("%v\n 内部错误或还没有设置过账号信息", err)
		}
		currentInfo, err = getCtlInfo(currentLogCtlCtx, currentLogCtlCtx.Current, "")
		return err
	}
	return nil
}

func getCtlInfo(ctx *logCtlCtx, user string, repoName string) (*logCtlInfo, error) {
	if ctx.Data == nil {
		return nil, fmt.Errorf("获取用户信息失败或无此用户: %s ，请确认是否设置了正确的账号", user)
	}
	userData := (*ctx.Data)[user]
	if userData == nil {
		return nil, fmt.Errorf("获取用户信息失败或无此用户: %s ，请确认是否设置了正确的账号", user)
	}

	info := &logCtlInfo{}

	info.User = user
	info.Ak, _ = Decrypt(userData.AK)
	info.Sk, _ = Decrypt(userData.SK)
	info.Range = userData.Range
	if info.Range < 1 {
		info.Range = 5
	}
	if len(repoName) != 0 {
		info.RepoName = repoName
	} else {
		info.RepoName = userData.Repo
	}
	repoData := (*userData.Data)[info.RepoName]
	if repoData == nil {
		return info, nil
	}
	info.Repo = repoData.Repo
	info.Log = repoData.Log
	return info, nil
}

func storeInfo(v *logCtlInfo) (err error) {
	if v == nil {
		return fmt.Errorf("待保存的参数 *logCtlInfo 为 nil")
	}
	ctxDataMap := currentLogCtlCtx.Data
	if ctxDataMap == nil {
		temp := make(map[string]*logCtlCtxData)
		ctxDataMap = &temp
	}

	ctxData := (*ctxDataMap)[v.User]
	if ctxData == nil {
		ctxData = &logCtlCtxData{}
	}
	ctxData.AK, _ = Encrypt(v.Ak)
	ctxData.SK, _ = Encrypt(v.Sk)
	ctxData.Range = v.Range
	ctxData.Repo = v.RepoName

	ctxDataDataMap := ctxData.Data
	if ctxDataDataMap == nil {
		temp := make(map[string]*logCtlCtxRepoData)
		ctxDataDataMap = &temp
	}

	repoData := &logCtlCtxRepoData{}
	repoData.Repo = v.Repo
	repoData.Log = v.Log

	(*ctxDataDataMap)[v.RepoName] = repoData
	ctxData.Data = ctxDataDataMap

	(*ctxDataMap)[v.User] = ctxData
	currentLogCtlCtx.Data = ctxDataMap

	storeCtx(currentLogCtlCtx)
	return
}

func storeCtx(ctx *logCtlCtx) (err error) {
	bytes, err := json.Marshal(ctx)
	if err == nil && bytes != nil {
		err = ioutil.WriteFile(userHomeDir()+"/"+_tempFile, bytes, 0666)
		if err != nil {
			log.Infoln("Opps....", err)
		}
	}
	return
}

func buildClient() (err error) {
	if logdbClient == nil {
		queryLogCtlInfo()
		cfg := logdb.NewConfig().
			WithAccessKeySecretKey(currentInfo.Ak, currentInfo.Sk).
			WithEndpoint("https://logdb.qiniu.com").
			WithDialTimeout(30 * time.Second).
			WithResponseTimeout(120 * time.Second).
			WithLogger(base.NewDefaultLogger()).
			WithLoggerLevel(base.LogDebug)
		logdbClient, err = logdb.New(cfg)
	}
	return
}
