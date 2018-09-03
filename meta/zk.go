package meta


import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
    log "github.com/Sirupsen/logrus"
    "sync"
    "math/rand"
)


type Zk struct {
    host string
    zk *zk.Conn
    stop chan bool
    crate_mut  sync.Mutex
}


func (c* Zk) Re_new() {
	c.Close()
	c.Init(c.host)
}

func (c *Zk) Init(hostname string) error {

    c.crate_mut.Lock()

    defer c.crate_mut.Unlock()

    r_session := rand.New(rand.NewSource(time.Now().UnixNano()))
    a := r_session.Intn(18000 - 15000) + 15000

    conn, _, err := zk.Connect([]string{hostname}, time.Second * time.Duration(a) )
    if err != nil {
        fmt.Println("create zk err:%v",err)
        return err
    }

    c.zk = conn
    c.host = hostname
    c.stop = make(chan bool,0)

    log.Info("zk init success:",hostname)
    return nil
}

func (c*Zk) Create(path string,data string) error {

    var flags int32
    flags = 0
    var acls = zk.WorldACL(zk.PermAll)

    var data_byte = []byte( data )

    _,err_create := c.zk.Create(path,data_byte,flags,acls)

    if err_create != nil {
        fmt.Println(err_create)

        c.Re_new()
        return err_create
    }

    log.Info("create data success:",path)
    return nil
}

func (c *Zk) Set( path string,data []byte ) error {

    old_data, stat, err := c.zk.Get(path)
    if err != nil {
        //log.Error("*****set data******\n",err,path)
        c.Create(path,"")
    }

    old_data_string := string( old_data )

    log.Info("zk old data:",old_data_string)
    log.Info("zk new data:",string(data) )

    if old_data_string != string(data) {
        stat, err = c.zk.Set(path, data, stat.Version)
        log.Info("zk set data:",stat,err)
    }
    if err != nil {
    	c.Re_new()
    }
    return err
}


func (c *Zk) Close() {
    if c.zk != nil {
        c.zk.Close()
    }
}



func (c *Zk) Watch(path string,res chan []byte) {

    var co_retry <-chan time.Time

    loop:
    for {
        data, tag, ch, err := c.zk.GetW(path)
        if err != nil {
            log.Error("GetW path :%v Error: %v", path , err)
            co_retry = time.After(time.Second * 60)
        } else {
            res <- data
            co_retry = nil
        }
        select {
            case <-co_retry:
                log.Warn("60 seconds end ,We try to watch ref path :%v again \n ",path)
            case <-c.stop:
                log.Warn("stop RunBody path:%v",path )
                break loop
            case <-ch:
                log.Info("Watch Path  has changed Event:  ", path,tag )
        }
    }

 
}


