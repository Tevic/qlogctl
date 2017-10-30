package log

var Logger = New()

func Verbose(msg ...interface{}) {
	Logger.Verbose(msg...)
}

func Verboseln(msg ...interface{}) {
	Logger.Verboseln(msg...)
}

func Verbosef(format string, msg ...interface{}) {
	Logger.Verbosef(format, msg...)
}

func Debug(msg ...interface{}) {
	Logger.Debug(msg...)
}

func Debugln(msg ...interface{}) {
	Logger.Debugln(msg...)
}

func Debugf(format string, msg ...interface{}) {
	Logger.Debugf(format, msg...)
}

func Info(msg ...interface{}) {
	Logger.Info(msg...)
}

func Infoln(msg ...interface{}) {
	Logger.Infoln(msg...)
}

func Infof(format string, msg ...interface{}) {
	Logger.Infof(format, msg...)
}

func Warn(msg ...interface{}) {
	Logger.Warn(msg...)
}

func Warnln(msg ...interface{}) {
	Logger.Warnln(msg...)
}

func Warnf(format string, msg ...interface{}) {
	Logger.Warnf(format, msg...)
}

func Error(msg ...interface{}) {
	Logger.Error(msg...)
}

func Errorln(msg ...interface{}) {
	Logger.Errorln(msg...)
}

func Errorf(format string, msg ...interface{}) {
	Logger.Errorf(format, msg...)
}
