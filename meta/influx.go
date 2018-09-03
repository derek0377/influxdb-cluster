package meta


import (
	"net/http"
	"net"
	"time"
	"crypto/tls"
	"github.com/influxdata/influxdb/models"
	"fmt"
	"bytes"
	"compress/gzip"
	log "github.com/Sirupsen/logrus"
	"net/url"
	"io/ioutil"
	"github.com/influxdata/influxdb/query"
	"encoding/json"
	"github.com/beeker1121/goque"
	//"github.com/infP/midd"
	"os"
	"math/rand"
	"strings"
)

var (
	_ = fmt.Sprintf("")
	FlushInterval = 1 * time.Second
	MAX_LEN = 2000
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
)



type Influx struct {
	Addr string 
	httpClient *http.Client
	stop chan bool
	httpAddr string
	queryAddr string
	pingAddr string

	Read bool
	Write bool

	queue_path string
	queue   *goque.Queue
	worker  int
	worker_stop map[int] []chan bool

	flag bool

	queue1 chan *Data
}

type Response struct {
	Results []*query.Result  `json:"results"`
	//Err     error
	Index     int
}


func CreatePath(path string) {

    _, err := os.Stat(path)

    if err != nil  {
        err := os.MkdirAll(path, 0711)

        if err != nil {
            log.Error("Error creating directory",path)
            return
        }
    }
}


func (i *Influx) Init(add string,path string, worker int) {
	i.queue1 = make(chan *Data)

	i.Addr = add
	i.httpClient = httpClient
	i.stop = make(chan bool)
	i.httpAddr = fmt.Sprintf("http://%s/write", i.Addr)
	i.queryAddr = fmt.Sprintf("http://%s/query", i.Addr)
	i.pingAddr = fmt.Sprintf("http://%s/ping", i.Addr)

	i.Read = true
	i.Write = true
	i.queue_path = fmt.Sprintf("%s/%s",path,add)
	CreatePath(i.queue_path)
	i.queue,_ = goque.OpenQueue(i.queue_path)
	i.worker = worker
	i.worker_stop = map[int] []chan bool{}
	i.flag = false

	for k := 0; k < i.worker; k ++ {
		i.worker_stop[k] =  []chan bool{}
		i.worker_stop[k] = append(i.worker_stop[k], make(chan bool))

		go i.RunSer( k,i.worker_stop[k][0] )
	}
	go i.alive(  i.stop )

	log.Info("run influxdb Ser:",i.Addr," worker:",i.worker)
}

func (i *Influx) Stop() {
	if i.queue != nil {
		i.queue.Close()
	}
	i.stop <- true
	for k,_ := range i.worker_stop {
		i.worker_stop[k][0] <- true
		//i.worker_stop[k][1] <- true
	}

	log.Info("close influxdb:", i.Addr)
}


func (i *Influx) PutPointsBytes(ret []models.Point, db string, rp string) {

	//count := len(ret)

	//_, err := i.queue.Enqueue( i.PointsString(ret) )
	//if err != nil {
	//	log.Error("Enqueue err:",err)
	//}

	//log.Info("PutPoints addr:", i.Addr," db:",db," points:", count  ," err:",err)
}


type Data struct {
	Name string
	Dd   []byte
	Leng int
}


func ( i *Influx) PointsString(points models.Points) ([]byte,int) {
	zbuf := bytes.NewBuffer(nil)

	c := 0

	for _, p := range points {
		t := p.String()

		zbuf.Write( []byte(t) )
		
		if _, err := zbuf.Write([]byte("\n")); err != nil {
			break
		}
		c = c + 1
	}
	return zbuf.Bytes(),c
}

func (i *Influx) PutPoints(ret []models.Point,db string,rp string) {

	b,c :=  i.PointsString(ret)


	a := Data{  Name: fmt.Sprintf("%s:%s",db,rp), Dd: b,Leng:c   }

	i.queue1 <- &a
	//_, err := i.queue.EnqueueObject( a )

	//if err != nil {
	//	log.Error("Enqueue err:",err)
	//}
}

func dialTimeout(network, addr string) (net.Conn, error) {
	var timeout = time.Duration(2 * time.Second)
    return net.DialTimeout(network, addr, timeout)
}

func (i *Influx) alive( stop chan bool ) {

	i.flag = true

	transport := http.Transport{
        Dial: dialTimeout,
    }

    client := http.Client{
        Transport: &transport,
    }

	tim := time.Tick(time.Second* 5)
	failCount := 0

	loop:
	for {
		select {
			case <- tim:

			    resp, err := client.Get(i.pingAddr)
			    if err != nil {
			    	failCount = failCount + 1 
			    	if failCount >= 3 {
			    		log.Info("ping fail:",i.pingAddr)
			    		i.flag = false
			    		if resp != nil {
			    			resp.Body.Close()
			    		}
			    		continue
			    	}
			    }

			    if resp == nil {
			    	failCount = failCount + 1 
			    	continue
			    }
			    if resp.StatusCode/100 != 2 {
			    	resp.Body.Close()
			    	failCount = failCount + 1
			    	if failCount >= 3 {
			    		log.Info("ping fail:",i.pingAddr)
			    		i.flag = false
			    		continue
			    	}
			    }

			    if failCount > 0 {
			    	i.flag = true
			    	failCount = 0
			    }
				resp.Body.Close()

				//log.Info("ping:",i.pingAddr," failCount:",failCount," flag:",i.flag)
			case <- stop:
				break loop
		}
	}
}

func (i *Influx) Flag() bool {
	return i.flag
}

func (i *Influx) RunSer(count int,stop chan bool) {

	total := map[string] *bytes.Buffer{}

	num := rand.Int31n(999)
	time.Sleep(time.Duration(num) * time.Millisecond)


	startCollect := time.Tick(FlushInterval)

	tc := map[string] int{}

	queue1 := make(chan *Data)

	queue1 = i.queue1
	loop:
	for {
		select {

			case <- startCollect: 
				if i.flag == false {
					if queue1 != nil {
						queue1 = nil
					}
					continue
				} else {
					if queue1 == nil {
						queue1 = i.queue1
					}
				}

				for k,_ := range total {
					//fmt.Printf("****len**kkk*%v\n", k )
					i.WritePointsBytes(total[k].Bytes(),k)
					total[ k  ].Reset()
					delete( total,k  )
				}
			case <- stop:
				break loop
			case ret := <- queue1:
				if i.flag == false {
					continue
				}
					
				//长度太长 直接发出去
				if ret.Leng >= MAX_LEN {
					//fmt.Printf("****directory***%v\n", ret.Leng )
					i.WritePointsBytes( ret.Dd,ret.Name)
					continue
				}

				_,ok := total[ret.Name]					
				if ok == false {
					total[ret.Name] = bytes.NewBuffer(nil)
					tc[ ret.Name ] = 0
				}

				tc[ ret.Name ] = tc[ ret.Name ] + ret.Leng
				total[ret.Name].Write( ret.Dd )

				//长度太长 直接发出去
				if tc[ ret.Name ] > MAX_LEN {
					//fmt.Printf("*******3******\n")
					i.WritePointsBytes(total[ ret.Name  ].Bytes(), ret.Name )
					total[ ret.Name  ].Reset()
					
					delete( total, ret.Name )
				}
		}
	}
	log.Info("close RunSer",i.Addr," worker no:",count)
}


func ( i *Influx) mustGzipPoints(points models.Points, precision string) []byte {
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

func ( i *Influx) WritePointsBytes(ret []byte,dbname string ) {

	//start := time.Now()

	u, err := url.Parse(i.httpAddr)
	
	if err != nil {
		log.Errorf("failed to parse influxdb instance addr: %v", err)
		return
	}
	p := url.Values{}

	t := strings.Split(dbname,":")

	p.Add("db", t[0])
	p.Add("rp", t[1])
	//p.Add("precision", precision)
	u.RawQuery = p.Encode()

	zbuf := bytes.NewBuffer(nil)
	gz := gzip.NewWriter(zbuf)
	gz.Write( ret )
	gz.Flush()
	gz.Close()

	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer( zbuf.Bytes() ) )
	if err != nil {
		log.Errorf("NewRequest Error: %v", err)
		return
	}

	// all data should be gzip encoded.
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := i.httpClient.Do(req)
	if err != nil {
		log.Errorf("influx.PostPoints - post data failed: %v", err)
		//errCh <- err
		return
	}

	//log.Info("write data cost time:", time.Now().Sub(start),"  addr:",i.httpAddr)

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Error("status code not 2xx",err)
			return
		} 
	}	
}

func ( i *Influx) WritePoints(points models.Points, errCh chan error,dbname string, rp string, precision string ) {

	start := time.Now()

	u, err := url.Parse(i.httpAddr)
	
	if err != nil {
		log.Errorf("failed to parse influxdb instance addr: %v", err)
		errCh <- err
		return
	}
	p := url.Values{}
	p.Add("db", dbname)
	p.Add("rp", rp)
	p.Add("precision", precision)
	u.RawQuery = p.Encode()


	by := i.mustGzipPoints( points, precision )

	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer( by ) )
	if err != nil {
		log.Errorf("NewRequest Error: %v", err)
		errCh <- err
		return
	}

	// all data should be gzip encoded.
	req.Header.Add("Content-Encoding", "gzip")

	//fmt.Printf("******%+v*\n", req  )
	resp, err := i.httpClient.Do(req)
	if err != nil {
		log.Errorf("influx.PostPoints - post data failed: %v", err)
		//i.PutPoints(points,dbname)
		errCh <- err
		return
	}

	log.Info("write data cost time:", time.Now().Sub(start),"  addr:",i.httpAddr)

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		//i.PutPoints(points,dbname)
		
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Error("status code not 2xx",err)
			errCh <- err
			return
		} 
	}
	errCh <- nil	
}



func ( i *Influx) Query(dbname, rpname, sql, epoch, chunk string, ret chan *Response,index int)   {

	if i.flag == false {
		ret <- &Response{Results: []*query.Result{}, Index: index}
		return
	}

	start := time.Now()

	p := url.Values{}
	p.Add("db", dbname)
	p.Add("rp", rpname)
	p.Add("epoch", epoch)
	p.Add("chunk", chunk)
	p.Add("q", sql)


	req, err := http.NewRequest("GET", i.queryAddr, nil)
	if err != nil {
		log.Errorf("NewRequest Error: %v", err)
		ret <- &Response{Results: []*query.Result{}, Index: index}
		return
	}

	req.URL.RawQuery = p.Encode()

	//req.Header.Add("Accept-Encoding", "gzip")

	resp, err := i.httpClient.Do(req)

	if resp == nil {
		ret <- &Response{Results: []*query.Result{}, Index: index}
		return		
	}

	defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
		ret <- &Response{Results: []*query.Result{}, Index: index}
		return
    }

    result := Response{} 
    err = json.Unmarshal ( body , &result ) 
    result.Index = index

    if err != nil {
    	log.Error("Unmarshal fail,",err)
	   	ret <- &Response{Results: []*query.Result{}, Index: index}
		return 
    }
   
	if resp.StatusCode/100 != 2 {
		
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Error("status code not 2xx",err)
			ret <- &Response{Results: []*query.Result{}, Index: index}
			return
		} 
	}
	ret <- &result
	log.Info("post time cost:",time.Now().Sub(start),", add:",i.queryAddr," sql:",sql," db:",dbname)
	return 	
}




