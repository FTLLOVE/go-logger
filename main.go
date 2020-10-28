package main

import (
	"bufio"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type LogProcess struct {
	rc chan []byte
	wc chan *Message
	r  Reader
	w  Write
}

type Message struct {
	TimeLocal                    time.Time
	ByteSent                     int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

type Reader interface {
	read(rc chan []byte)
}

type Write interface {
	write(wc chan *Message)
}

type ReadFromFile struct {
	path string
}

type WriteToInfluxDB struct {
	influxDBDsn string
}

//TODO 写入到MongoDB
//type WriteToMongoDB struct {
//	mongoConfig string
//}

// 读取模块
func (r *ReadFromFile) read(rc chan []byte) {
	// 读取文件
	file, err := os.Open(r.path)
	if err != nil {
		fmt.Printf("os.Open() fail, err: %v\n", err.Error())
		return
	}
	file.Seek(0, 2)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {
			fmt.Printf("reader.ReadBytes() fail, err : %v\n", err.Error())
			return
		}
		rc <- line[:len(line)-1]
	}

}

// 解析模块
func (l *LogProcess) process() {
	reg := `([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`
	r := regexp.MustCompile(reg)
	for v := range l.rc {
		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14 {
			fmt.Printf("reg FindStringSubmatch(),err: %v \n", string(v))
			continue
		}
		message := &Message{}
		loc, _ := time.LoadLocation("Asia/Shanghai")
		locationTime, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			fmt.Printf("time.ParseInLocation fail, err : %v \n", ret[4])
			continue
		}
		// 172.0.0.12 - - [04/Mar/2018:13:49:52 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-" "KeepAliveClient" "-" 1.005 1.854
		message.TimeLocal = locationTime
		byteSent, err := strconv.Atoi(ret[8])
		if err != nil {
			fmt.Printf("strconv.Atoi() fail,err: %v \n", ret[8])
			continue
		}
		message.ByteSent = byteSent

		// "GET /foo?query=t HTTP/1.0"
		splitSlice := strings.Split(ret[6], " ")
		if len(splitSlice) != 3 {
			fmt.Printf("strings.Split fail,err: %v\n", splitSlice)
			continue
		}
		message.Method = splitSlice[0]
		urlPath, err := url.Parse(splitSlice[1])
		if err != nil {
			log.Printf("urlPath.Parse() fail,err: %v \n", splitSlice[1])
			continue
		}
		message.Path = urlPath.Path
		message.Scheme = ret[5]
		message.Status = ret[7]
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		message.UpstreamTime = upstreamTime
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.RequestTime = requestTime
		l.wc <- message
	}
}

// 写入模块->influxDB
func (w *WriteToInfluxDB) write(wc chan *Message) {
	configSlice := strings.Split(w.influxDBDsn, "@")
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     configSlice[0],
		Username: configSlice[1],
		Password: configSlice[2],
	})
	if err != nil {
		log.Printf("client.NewHTTPClient() fail,err: %v \n", err.Error())
		return
	}
	for v := range wc {
		batchPoints, err := client.NewBatchPoints(client.BatchPointsConfig{
			Precision: configSlice[4],
			Database:  configSlice[3],
		})
		if err != nil {
			log.Printf("client.NewBatchPoints() fail,err: %v \n", err.Error())
			return
		}
		tags := map[string]string{
			"Path":   v.Path,
			"Method": v.Method,
			"Scheme": v.Scheme,
			"Status": v.Status,
		}
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"ByteSent":     v.ByteSent,
		}
		newPoint, err := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if err != nil {
			log.Printf("client.NewPoint() fail,err: %v \n", err.Error())
			return
		}
		batchPoints.AddPoint(newPoint)
		if err := c.Write(batchPoints); err != nil {
			log.Printf("c.Write fail,err: %v \n", err.Error())
			return
		}
		log.Printf("write success")
	}
}

//TODO 写入模块->MongoDB
//func (w *WriteToMongoDB) write(wc chan string) {
//	fmt.Println("写入到MongoDB的数据是: ", <-wc)
//}

func main() {
	readFromFile := &ReadFromFile{
		path: "./file/access.log",
	}
	wToInfluxDB := &WriteToInfluxDB{
		influxDBDsn: "http://localhost:8086@admin@123456@my_test@s",
	}
	//TODO 写入到MongoDB
	//wToMongoDB := &WriteToMongoDB{mongoConfig: ""}
	lp := &LogProcess{
		rc: make(chan []byte, 200),
		wc: make(chan *Message, 200),
		r:  readFromFile,
		w:  wToInfluxDB,
	}
	go lp.r.read(lp.rc)
	for i := 0; i < 4; i++ {
		go lp.process()
	}
	for i := 0; i < 4; i++ {
		go lp.w.write(lp.wc)
	}
	time.Sleep(time.Second * 60)
}
