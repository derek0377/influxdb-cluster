package main

import (
	//"bytes"
	//"encoding/json"
	"flag"
	"fmt"
	//"net/http"
	"os"
	"github.com/infP/config"
	"github.com/infP/http"
	//"errors"
	//"io/ioutil"
	"os/signal"
	"syscall"
	//"time"
	log "github.com/Sirupsen/logrus"
	_"net/http/pprof"
)


/***
zk:
1 slot mapping
2 servers list
3 db lists
***/
var (
	ata_path  string
	conf = config.Conf{}
)  

func init() {

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:      true,
		QuoteEmptyFields: true,
		FullTimestamp:    true,
	})

	log.SetOutput(os.Stdout)
	//log.SetLevel(log.DebugLevel)
}

func run(conf *config.Conf) {

	apiSrv, err := http.NewApiService(conf)
	if err != nil {
		log.Fatalln("Failed to start api service: %v ", err)
	}

	go apiSrv.Run()
	return
}

func main() {

    ata_path := flag.String("conf","/root/redis_conf/redis_conf","path")
    flag.Parse()

    conf := config.Conf{}
    conf.Get_conf(*ata_path)


	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)

	run( &conf )

	for {
		select {
		case <-sigCh:
			fmt.Fprintf(os.Stdout, "got a hup signal \n")
		}
	}
}



