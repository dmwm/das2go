package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	logs "github.com/sirupsen/logrus"
)

// global variable
var Profiler *bufio.Writer

func InitFunctionProfiler(fname string) {
	// extract path from given file name
	path, err := os.Getwd()
	if err != nil {
		logs.WithFields(logs.Fields{
			"err": err,
			"dir": path,
		}).Error("failed to get local working directory")
		return
	}
	if fname == "" {
		fname = "das-profile.log"
	} else {
		arr := strings.Split(fname, "/")
		path = strings.Join(arr[:len(arr)-1], "/")
		fname = arr[len(arr)-1]
	}
	// create the log directory
	if err := os.MkdirAll(path, 0755); err != nil {
		logs.WithFields(logs.Fields{
			"err": err,
			"dir": path,
		}).Error("failed to make the directory")
		return
	}
	// open the log file
	fname = fmt.Sprintf("%s/%s", path, fname)
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logs.WithFields(logs.Fields{
			"err":  err,
			"file": fname,
		}).Error("failed to open file")
		return
	}
	Profiler = bufio.NewWriter(file)
	if WEBSERVER != 0 {
		logs.WithFields(logs.Fields{
			"file": fname,
		}).Info("DAS profiler")
	}
}

// Latency Measurement of individual component of the codebase
// https://medium.com/swlh/easy-guide-to-latency-measurement-in-golang-38c3297ebbd2
// Usage, put the following statement in any function we need to measure:
// defer measureTime("funcName")
func MeasureTime(funcName string) func() {
	start := time.Now()
	return func() {
		if Profiler != nil {
			fmt.Fprintf(Profiler, "%s %s %v \n", start.Format("20060102150405"), funcName, time.Since(start))
			Profiler.Flush()
		}
	}
}
