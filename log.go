package delayTask_etcd

import (
	"log"
	"os"
	"runtime"
	"strconv"
)

type Logger interface {
	Info(format string, v ...interface{})
	Error(format string, v ...interface{})
}

var _ Logger = (*stdOutLog)(nil)

type stdOutLog struct {
	log   *log.Logger
	level int
}

func newStdOutLog() Logger {
	l := stdOutLog{}
	l.log = log.New(os.Stdout, "delayTask-etcd", log.Lmicroseconds|log.Lmsgprefix)
	return &l
}

func (m *stdOutLog) Info(format string, v ...interface{}) {
	// 获取调用者文件和行号
	_, _, line, _ := runtime.Caller(2)

	m.log.Printf(" [I] line:"+strconv.Itoa(line)+" "+format, v...)
}

func (m *stdOutLog) Error(format string, v ...interface{}) {
	_, _, line, _ := runtime.Caller(2)

	m.log.Printf(" [E] line:"+strconv.Itoa(line)+" "+format, v...)
}
