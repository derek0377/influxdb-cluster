package meta

import (
	"github.com/infP/config"
	log "github.com/Sirupsen/logrus"
	"encoding/json"
	"reflect"
	"strings"
	"strconv"
	//"github.com/infP/service"
	//"github.com/influxdata/influxdb/models"
	"fmt"
	"sync"
	"math/rand"
	"time"
)

var (
	_ = fmt.Sprintf("")
)

type Cluster struct  {
	Conf        *config.Conf
	Slot        *Slot
	Influxs     map[string] * Influx
	Zk          *Zk
	slotMachine []string
	stop chan bool
	udp         map[string] * Udp
	Migra       *Slot
	Inf_sync    sync.Mutex

	DbShard    *DbShard

	ClassIds  map[int] []string
}

func ( c *Cluster) FetchRandInfluxs() []*Influx {

	tmp := []*Influx{}

	for k,_ := range c.ClassIds {

		_len := len( c.ClassIds[k] )
		_len = rand.Intn(_len)

		machine := c.ClassIds[k][ _len ]

		in,_ := c.Influxs[machine]

		tmp = append( tmp,in )
	}
	return tmp
}

func ( c *Cluster) FetchAllInfluxs() []*Influx {

	tmp := []*Influx{}
	for _,v := range c.Influxs {
		tmp = append(tmp, v)
	}
	return tmp
}

func ( c *Cluster) FetchWriteInflux(ip string) *Influx {
	k,ok := c.Influxs[ip]
	if ok == false {
		return nil
	}

	if k.Write == false {
		return nil
	}

	return k
}

func ( c *Cluster) FetchReadInflux(ip string) *Influx {
	k,ok := c.Influxs[ip]
	if ok == false {
		return nil
	}

	if k.Read == false {
		return nil
	}

	if k.Flag() == false {
		return nil
	}
	return k
}

func ( c *Cluster) RefreshSlot(tmp []string) {
	ret := c.refreshSlot(tmp)
	c.Slot.SetSlot( ret )
}

func (c *Cluster) RefreshIns(list []string) {

	c.Inf_sync.Lock()
	defer c.Inf_sync.Unlock()

	tmp := map[string]int{}

	for _,v := range list {

		if v[0:1] == "#" {
			continue
		}

		ret := strings.Split(v," ")
		if len(ret) < 2 {
			continue
		}
		inst := strings.Trim(ret[0]," ")
		tmp[inst] = 0
	}	

	for k,_ := range c.Influxs {
		_,ok := tmp[k]
		if ok == false {
			c.Influxs[k].Stop()
			delete(c.Influxs,k)
			log.Info("delete Influxdb,",k)
		}
	}

	for k,_ := range tmp {
		_,ok := c.Influxs[k]
		if ok == false {
			c.Influxs[k] = &Influx{}
			c.Influxs[k].Init(k, c.Conf.Persi_queue,c.Conf.Write_worker)
			log.Info("add Influx:",k)
		}
	}
}


//to do,all slot should be cover
func ( c *Cluster) refreshSlot(tmp []string) map[uint16] []string {

	tmpSlot := map[uint16] []string{}

	for _,v := range tmp {

		if v[0:1] == "#" {
			continue
		}

		ret := strings.Split(v," ")
		if len(ret) < 2 {
			continue
		}
		inst := strings.Trim(ret[0]," ")

		for i := 1; i< len(ret); i ++ {
			slot := ret[ i ]
			tmp1 := strings.Split(slot,"-")
			if len(tmp1) != 2 {
				log.Error("sloting is not right",slot)
				continue
			}

			s,err := strconv.Atoi(tmp1[0])
			if err != nil {
				log.Error("start slot not right",slot)
				continue
			}

			e,err := strconv.Atoi(tmp1[1])
			if err != nil {
				log.Error("end slot not right",slot)
				continue
			}

			for i := s; i <= e; i++ {
				_,ok := tmpSlot[ uint16(i) ]
				if ok == false {
					tmpSlot[ uint16(i) ] = []string{}
				}
				tmpSlot[ uint16(i)  ] = append( tmpSlot[ uint16(i) ], inst  )
			}
		}
	}
	return tmpSlot
}

func (c *Cluster) Close() {
	c.Zk.Close()
}

func ( c *Cluster ) Init(conf *config.Conf) {
	c.Conf = conf
	c.slotMachine = []string{}
	c.stop = make(chan bool)

	log.Info("cluster get conf, zk is:",conf.Zk)
	log.Info("cluster get conf, zk path:",conf.Zk_path)

	c.Slot = &Slot{}
	c.Slot.Init()

	c.Influxs = map[string] *Influx{}

	c.udp     = map[string] * Udp{}

	c.Migra   = &Slot{}
	c.Migra.Init()


	c.Zk = &Zk{}
	c.Zk.Init( c.Conf.Zk )

	c.DbShard = &DbShard{}
	c.DbShard.Init()

	c.ClassIds = map[int] []string{}

	go c.StartWatchSlot()
	go c.StartWatchUdp()
	go c.StartWatchMigra()
	go c.StartWatchSerStatus()
	go c.StartWatchDbShard()

	// poc code
	go c.StartWatchTrigger()
}

func (c *Cluster) RefreshDbslot(coming []string) {
	c.DbShard.Set(coming)
}

// poc code
func ( c *Cluster ) StartWatchTrigger() {

	tmp := make(chan []byte,800 )

	time.Sleep( 5 * time.Second )

	go c.Zk.Watch(c.Conf.Trigger,tmp)

	loop:
	for {
        select {
            case ret := <- tmp:
            	log.Info("catch trigger change:", string(ret) )

            	tmp1 := []string{}

			    err := json.Unmarshal ( ret , &tmp1 ) 

			    if err != nil {
			    	log.Error(" trigger context from zk Unmarshal fail:",err)
			    	continue
			    } 

			    for _,v := range tmp1 {

			    	ret := strings.Split(v," ")
			    	m := strings.Split( ret[0], ":" )
			    	instance := ret[1:]
			    	for _, measu := range m {
			    		ss := c.Slot.FetchSlot([]byte(measu))
			    		c.Slot.Append(ss,instance)

			    		for _,vv := range instance {
							_,ok := c.Influxs[vv]
							if ok == false {
								c.Influxs[vv] = &Influx{}
								c.Influxs[vv].Init(vv, c.Conf.Persi_queue,1)
								log.Info("add trigger consumer:",vv)
							}
						}
			    	}

			    }

			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}

func ( c *Cluster ) StartWatchDbShard() {

	DbShard := make(chan []byte,800 )

	go c.Zk.Watch(c.Conf.Db_shard,DbShard)

	loop:
	for {
        select {
            case ret := <- DbShard:
            	log.Info("catch dbShard change:", string(ret) )
            	tmp := []string{}
			    err := json.Unmarshal ( ret , &tmp ) 

			    if err != nil {
			    	log.Error("Unmarshal fail:",err)
			    	continue
			    } else {    	
			    	c.RefreshDbslot (tmp)

			    }
			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}

func ( c *Cluster ) StartWatchSlot() {

	slotMachine := make(chan []byte,800 )

	go c.Zk.Watch(c.Conf.Zk_path,slotMachine)

	loop:
	for {
        select {
            case ret := <- slotMachine:
            	log.Info("catch slot change:", string(ret) )
            	tmp := []string{}
			    err := json.Unmarshal ( ret , &tmp ) 

			    if err != nil {
			    	log.Error("Unmarshal fail:",err)
			    	continue
			    } else {
			    	result := reflect.DeepEqual(c.slotMachine, tmp)
			    	if result == true {
			    		log.Info("catch zk the same as old,",tmp)
			    		continue
			    	}

			    	c.RefreshSlot(tmp)
			    	c.RefreshIns(tmp)
			    	c.slotMachine = tmp
			    }
			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}

func (c *Cluster) CheckUdp(coming []string) {
	tmp := map[string] []string {}

	for _,v :=range coming {

		if len(v) <= 1 {
			continue
		}

		if v[0:1] == "#" {
			continue
		}

		t := strings.Split(v," ")
		if len(v) < 3 {
			continue
		}

		_,err := strconv.Atoi( t[2])
		if err != nil {
			continue
		}

		port := t[0]
		db := t[1]

		tmp[port] = []string{ db, t[2] }
	}

	for k,v := range c.udp {
		_,ok := tmp[k]
		if ok == false {
			log.Info("close udp:",k)
			v.Close()
			delete(c.udp, k)
		} 
	}

	for k,v := range tmp {
		_,ok := c.udp[ k ]
		if ok == false {
			log.Info("new udp:",k," worker:",v[1])
			c.udp[k] = &Udp{}
			c.udp[k].Init(k,v[0],v[1],c)
		} else {
			if v[0] != c.udp[k].Db() {
				c.udp[k].Close()

				c.udp[k] = &Udp{}
				c.udp[k].Init(k,v[0],v[1],c)
				log.Info("renew udp:",k," worker:",v[1])
			} else if v[1] != c.udp[k].Worker() {
				c.udp[k].Close()

				c.udp[k] = &Udp{}
				c.udp[k].Init(k,v[0],v[1],c)
				log.Info("renew udp:",k," worker:",v[1])
			}
		}
	}
}

func ( c *Cluster ) StartWatchUdp() {

	udp := make( chan []byte,2000 )

	go c.Zk.Watch(c.Conf.Udp_path,udp)

	loop:
	for {
        select {
            case ret := <- udp:
            	log.Info("catch udp change:", string(ret) )
            	tmp := []string{}
			    err := json.Unmarshal ( ret , &tmp ) 

			    if err != nil {
			    	log.Error("Unmarshal fail:",err)
			    	continue
			    } else {
			    	c.CheckUdp(tmp)
			    }
			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}


func (c *Cluster) CheckMigr(coming []string) {
	ret := c.refreshSlot(coming)
	c.Migra.Init()

	totalSlot := 0
	for _,_ = range ret {
		totalSlot = totalSlot + 1
	}

	log.Info("catch Migra conf, total slot is:",totalSlot)
	c.Migra.SetSlot( ret )
}

func ( c *Cluster ) StartWatchMigra() {

	m := make( chan []byte,2000 )

	go c.Zk.Watch(c.Conf.Migra_slot,m)

	loop:
	for {
        select {
            case ret := <- m:
            	//log.Info("catch Migra_slot change:", string(ret) )
            	tmp := []string{}
			    err := json.Unmarshal ( ret , &tmp ) 

			    if err != nil {
			    	log.Error("Unmarshal fail:",err)
			    	continue
			    } else {
			    	c.CheckMigr(tmp)
			    }
			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}



func (c *Cluster) CheckSerStatus(list []string) {

	c.Inf_sync.Lock()
	defer c.Inf_sync.Unlock()

	tmp := map[string] []bool{}

	classIds := map[int] []string{}

	for _,v := range list {

		if v[0:1] == "#" {
			continue
		}

		ret := strings.Split(v," ")
		if len(ret) != 4 {
			continue
		}
		inst := strings.Trim(ret[0]," ")


		write_flag,_ := strconv.ParseBool( ret[1] )
		read_flag,_ := strconv.ParseBool( ret[2] )

		class_id,_ := strconv.Atoi( ret[3] )

		_,ok := classIds[class_id]

		if ok  == false {
			classIds[class_id] = []string{}
		}
		classIds[class_id] = append(classIds[class_id], inst)

		tmp[inst] = []bool{write_flag,read_flag}
	}

	c.ClassIds = classIds
	log.Info("class ids is:",c.ClassIds)


	for k,_ := range c.Influxs {
		_,ok := tmp[k]
		if ok == false {
			c.Influxs[k].Stop()
			delete(c.Influxs,k)
			log.Info("delete Influxdb,",k)
		}
	}

	for k,v := range tmp {
		_,ok := c.Influxs[k]
		if ok == false {
			c.Influxs[k] = &Influx{}
			c.Influxs[k].Init(k, c.Conf.Persi_queue,c.Conf.Write_worker)
			c.Influxs[k].Write = v[0]
			c.Influxs[k].Read = v[1]
			log.Info("add Influx:",k,"  write flag, read flag:",v)
		} else {
			if c.Influxs[k].Write != v[0] || c.Influxs[k].Read != v[1] {
				c.Influxs[k].Write = v[0]
				c.Influxs[k].Read = v[1]
				log.Info("change Influx write flag:",v[0]," read_flag:",v[1])
			}			
		}
	}
}

func ( c *Cluster ) StartWatchSerStatus() {

	m := make( chan []byte,5000 )

	go c.Zk.Watch(c.Conf.Ser_status,m)

	loop:
	for {
        select {
            case ret := <- m:
            	//log.Info("catch Migra_slot change:", string(ret) )
            	tmp := []string{}
			    err := json.Unmarshal ( ret , &tmp ) 

			    if err != nil {
			    	log.Error("Unmarshal fail:",err)
			    	continue
			    } else {
			    	c.CheckSerStatus(tmp)
			    }
			case <- c.stop:
				log.Info("stop cluster!")
				c.Close()
				break loop

        }		
	}
}