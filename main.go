package main

import (
	"fmt"
	"strings"
	"time"
)

type LogProcess struct {
	rc chan string
	wc chan string
	r  Reader
	w  Write
}

type Reader interface {
	read(rc chan string)
}

type Write interface {
	write(wc chan string)
}

type ReadFromFile struct {
	path string
}

type WriteToInfluxDB struct {
	influxDb string
}

// 读取模块
func (r *ReadFromFile) read(rc chan string) {
	rc <- "hello world"
}

// 解析模块
func (l *LogProcess) process() {
	data := <-l.rc
	l.wc <- strings.ToUpper(data)
}

// 写入模块->influxDB
func (w *WriteToInfluxDB) write(wc chan string) {
	fmt.Println("读取到的数据是: ", <-wc)
}

func main() {
	readFromFile := &ReadFromFile{
		path: "",
	}
	wToInfluxDB := &WriteToInfluxDB{}
	l := &LogProcess{
		rc: make(chan string),
		wc: make(chan string),
		r:  readFromFile,
		w:  wToInfluxDB,
	}
	go readFromFile.read(l.rc)
	go l.process()
	go wToInfluxDB.write(l.wc)
	time.Sleep(time.Second * 1)
}
