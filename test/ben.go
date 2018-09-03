package main

import (
	"net/http"
	"net"
	"time"
	"fmt"
	"net/url"
	"io/ioutil"
	"bytes"
	log "github.com/Sirupsen/logrus"
	"compress/gzip"
	"sync/atomic"
	"crypto/tls"
	"github.com/influxdata/influxdb/models"
	"os"
	"math/rand"
	"strconv"
)



var (
	//TODO: move to function, support config
	httpClient = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
	}}


	count uint64
	total uint64

	LEN = 500
)


func  mustGzipPoints(points models.Points, precision string) []byte {
	zbuf := bytes.NewBuffer(nil)

	gz := gzip.NewWriter(zbuf)
	for _, p := range points {
		if _, err := gz.Write([]byte(p.PrecisionString(precision))); err != nil {
			break
		}
		if _, err := gz.Write([]byte("\n")); err != nil {
			break
		}
	}
	gz.Flush()
	gz.Close()
	return zbuf.Bytes()
}


func generate(ret chan []byte,c int) {

	ddd := rand.Intn(1000)

	zbuf := bytes.NewBuffer(nil)

	gz := gzip.NewWriter(zbuf)

	co := 0

	//for k:=200;k >= 0; k -- {
	for k:=0;k<50000; k++ {
		//for j:=120; j >= 0; j -- {
		for j:=0; j <= 120; j ++ {
			t := time.Now().Add( - time.Second * time.Duration(  k*3600 + j*30 )  ).Unix()

			//fmt.Printf("***t********%v\n",t)
			// 10000 个 measrement
			for i:=1;i<=100; i ++ {
				//m := c * i
				co = co + 1
				a := []byte( fmt.Sprintf("meament_%s_%d,host=opshost%d,app=opsapp%d,pod=opspod%d,test=opstest%d value=%d %d000000000",os.Args[1],ddd,i,i,i,i,i,t) )

				//points, _ := models.ParsePointsWithPrecision(a, time.Now().UTC(), "")

				//fmt.Printf("data****%v*****%v\n", points[0].PrecisionString(""),parseError)

				//gz.Write([]byte(points[0].PrecisionString("")))
				gz.Write( []byte(a) )
				gz.Write([]byte("\n"))

				if co >= 100 &&  co % 100 == 0 {
					co = 0
					gz.Flush()
					gz.Close()

					ret <- zbuf.Bytes()
					
					zbuf = bytes.NewBuffer(nil)
					gz = gzip.NewWriter(zbuf)
				}
			}
		}
	}

}


func static() {
	startCollect := time.Tick(1*time.Second)

	var last uint64
	last = 0
	var l uint64

	l = 0
	
	//loop:
	for {

		select {
			case <- startCollect:
				fmt.Printf("****summary***%v***%v***%v\n", count - last, total - l, float64( total - l ) / float64( count - last ) )
				last = count
				l = total
			}
	}
}


func main() {

	count = 0
	total = 0
	tt := map[int] chan []byte{}

	tta,_:=strconv.Atoi( os.Args[2]  )

	for i:=1; i <= tta; i ++ {
		h := make( chan []byte )
		tt[ i ] = h
		go generate(h,i)
		//go send(h,i)
	}

    time.Sleep(5* time.Second)

 	for i:=1; i <= tta; i ++ {
		c,_ := tt[i]
    	go send(c,i)
	}

	static()
}

func send(ret chan []byte,i int) {

   //     end := time.Now()
	//loop:
	for {
		select {
			case data := <- ret:
//  fmt.Printf("*****tt***%v\n", time.Now().Sub( end  ) )
				post(data)
 //fmt.Printf("******end*****%v\n",time.Now() )
 //end = time.Now()
		}
	}

}

func post(ret []byte) {


	atomic.AddUint64(&count, uint64(LEN))

	//addr := "http://10.0.48.114:6666/write"
	//addr := "http://10.0.48.20:6666/write"

	var addr string

	if os.Args[1] == "0" {
		addr = "http://10.0.49.59:8086/write"
	}

	if os.Args[1] == "1" {
		addr = "http://10.0.48.20:6666/write"
	}

	start := time.Now().UnixNano()

	u, err := url.Parse(addr)
	
	if err != nil {
		log.Errorf("failed to parse influxdb instance addr: %v", err)
		//errCh <- err
		return
	}


	p := url.Values{}
	p.Add("db", "mydb")
	u.RawQuery = p.Encode()

	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer( ret ) )
	if err != nil {
		log.Errorf("NewRequest Error: %v", err)
		return
	}

	req.Header.Add("Content-Encoding", "gzip")

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf("influx.PostPoints - post data failed: %v\n", err)
		return
	}


	cost_time := (time.Now().UnixNano() - start)/1000000

	//log.Info("write data cost time:", cost_time)

	atomic.AddUint64(&total, uint64( cost_time   ))
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if r, err := ioutil.ReadAll(resp.Body); err != nil {
			fmt.Printf("status code not 2xx:%v\n",err)
			return
		}  else {
			fmt.Printf("***r*****%v\n", string(r)  )
		}
	}

}
