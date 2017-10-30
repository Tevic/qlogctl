package log

import (
	"fmt"
	"io"
	"os"
)

const (
	VERBOSE = iota
	DEBUG
	INFO
	WARN
	ERROR
	NONE
)

type logger struct {
	Level        int
	ErrorOut     io.Writer
	WarnOut      io.Writer
	InfoOut      io.Writer
	DebugOut     io.Writer
	VerboseOut   io.Writer
	PrefixFormat string
}

func New() *logger {
	return &logger{
		Level:        INFO,
		ErrorOut:     os.Stderr,
		WarnOut:      os.Stderr,
		InfoOut:      os.Stdout,
		DebugOut:     os.Stdout,
		VerboseOut:   os.Stdout,
		PrefixFormat: "",
	}
}

func (l *logger) Verbose(msg ...interface{}) {
	if l.Level > VERBOSE {
		return
	}
	fmt.Fprint(l.VerboseOut, msg...)
}

func (l *logger) Verboseln(msg ...interface{}) {
	if l.Level > VERBOSE {
		return
	}
	fmt.Fprintln(l.VerboseOut, msg...)
}

func (l *logger) Verbosef(format string, msg ...interface{}) {
	if l.Level > VERBOSE {
		return
	}
	fmt.Fprintf(l.VerboseOut, format, msg...)
}

func (l *logger) Debug(msg ...interface{}) {
	if l.Level > DEBUG {
		return
	}
	fmt.Fprint(l.DebugOut, msg...)
}

func (l *logger) Debugln(msg ...interface{}) {
	if l.Level > DEBUG {
		return
	}
	fmt.Fprintln(l.DebugOut, msg...)
}

func (l *logger) Debugf(format string, msg ...interface{}) {
	if l.Level > DEBUG {
		return
	}
	fmt.Fprintf(l.DebugOut, format, msg...)
}

func (l *logger) Info(msg ...interface{}) {
	if l.Level > INFO {
		return
	}
	fmt.Fprint(l.InfoOut, msg...)
}

func (l *logger) Infoln(msg ...interface{}) {
	if l.Level > INFO {
		return
	}
	fmt.Fprintln(l.InfoOut, msg...)
}

func (l *logger) Infof(format string, msg ...interface{}) {
	if l.Level > INFO {
		return
	}
	fmt.Fprintf(l.InfoOut, format, msg...)
}

func (l *logger) Warn(msg ...interface{}) {
	if l.Level > WARN {
		return
	}
	fmt.Fprint(l.WarnOut, msg...)
}

func (l *logger) Warnln(msg ...interface{}) {
	if l.Level > WARN {
		return
	}
	fmt.Fprintln(l.WarnOut, msg...)
}

func (l *logger) Warnf(format string, msg ...interface{}) {
	if l.Level > WARN {
		return
	}
	fmt.Fprintf(l.WarnOut, format, msg...)
}

func (l *logger) Error(msg ...interface{}) {
	if l.Level > ERROR {
		return
	}
	fmt.Fprint(l.ErrorOut, msg...)
}

func (l *logger) Errorln(msg ...interface{}) {
	if l.Level > ERROR {
		return
	}
	fmt.Fprintln(l.ErrorOut, msg...)
}

func (l *logger) Errorf(format string, msg ...interface{}) {
	if l.Level > ERROR {
		return
	}
	fmt.Fprintf(l.ErrorOut, format, msg...)
}

func (l *logger) SetLevel(level int) *logger {
	if level > NONE || level < VERBOSE {
		return l
	}
	l.Level = level
	return l
}

func (l *logger) SetErrorOut(out io.Writer) *logger {
	l.ErrorOut = out
	return l
}

func (l *logger) SetInfoOut(out io.Writer) *logger {
	l.InfoOut = out
	return l
}

func (l *logger) SetDebugOut(out io.Writer) *logger {
	l.DebugOut = out
	return l
}

func (l *logger) SetVerboseOut(out io.Writer) *logger {
	l.VerboseOut = out
	return l
}

func (l *logger) SetPrefixFormat(format string) *logger {
	l.PrefixFormat = format
	return l
}
