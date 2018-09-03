package main

import (
	//"bytes"
	//"encoding/json"
	"flag"
	"fmt"
	//"net/http"
	"os"
	"github.com/infP/config"
	"github.com/infP/meta"
	"encoding/json"
	//"io/ioutil"
	//"os/signal"
	"io"
	"strings"
	log "github.com/Sirupsen/logrus"
	"bufio"
)


var (
	ata_path  string
	conf = config.Conf{}
  _ = fmt.Sprintf("")
)  

func init() {

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:      true,
		QuoteEmptyFields: true,
		FullTimestamp:    true,
	})

	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}


func fetchContext(name string) []string {
    fi, err := os.Open(name)
    if err != nil {
        log.Info("open slot conf error:", err)
        return []string{}
    }

    defer fi.Close()

    ret := []string{}

    br := bufio.NewReader(fi)
    for {
        a, _, c := br.ReadLine()
        if c == io.EOF {
            break
        }
        ret = append(ret, strings.Trim(string(a),"\n"))
    }

    return ret	
}

func main() {

    ata_path := flag.String("conf","","path")
    slot_path := flag.String("slot","","slot")

    udp_path := flag.String("udp","","udp")

    ser_status := flag.String("ser_status","","ser_status")
    migra_slot := flag.String("migra_slot","","migra_slot")

    db_sharding := flag.String("db_sharding","","db_sharding")

    //poc code
    trigger := flag.String("trigger","","trigger")

    flag.Parse()

    zk := meta.Zk{}

    conf := config.Conf{}

    if *ata_path != "" {
      conf.Get_conf(*ata_path)

   	  zk.Init( conf.Zk  )

    }

    if *slot_path != "" {
     	ret := fetchContext(*slot_path)
     	value,_ := json.Marshal(ret)
     	zk.Set( conf.Zk_path, value )
    }

    if *udp_path != "" {
     	ret := fetchContext(*udp_path)
     	value,_ := json.Marshal(ret)
     	zk.Set( conf.Udp_path, value )
    }

    if *ser_status != "" {
      ret := fetchContext(*ser_status)
      value,_ := json.Marshal(ret)

      zk.Set( conf.Ser_status, value ) 
    }

    if *migra_slot != "" {
      ret := fetchContext(*migra_slot)
      value,_ := json.Marshal(ret)
      zk.Set( conf.Migra_slot, value ) 
    }

    if *db_sharding != "" {
      ret := fetchContext(*db_sharding)
      value,_ := json.Marshal(ret)
      zk.Set( conf.Db_shard, value ) 
    }
    
    if *trigger != "" {
      ret := fetchContext(*trigger)
      value,_ := json.Marshal(ret)
      zk.Set( conf.Trigger, value ) 
    }

    if *ata_path != "" {
      zk.Close()
    }

}



