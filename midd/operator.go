package midd

import (
	"github.com/influxdata/influxdb/models"
	"github.com/infP/meta"
	log "github.com/Sirupsen/logrus"
	"fmt"
	"errors"
	"time"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	//"github.com/abrander/influxdb/influxql"
	"strings"
	"math/rand"
	"runtime"
)

var (
	_ = fmt.Sprintf("")
	trace = make([]byte, 1024, 1024)
)


type Operator struct {
	Cluster        *meta.Cluster
	Slot           *meta.Slot
	Migra          *meta.Slot
	WriteTimeOut   time.Duration
}


var (
	MAX_KEY_SIZE = 200
	MAX_VAL_SIZE = 200
)



func (p *Operator) SetCluster(c *meta.Cluster) {
	p.Cluster = c
	p.Slot = p.Cluster.Slot
	p.WriteTimeOut = c.Conf.Write_timeout_dur
	p.Migra = p.Cluster.Migra
}



func (p *Operator) Query(dbname, rpname, sql, epoch, chunk string, timeout int) (meta.Response, error) {

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("query: Recover from panic: %s\n", r)
			count := runtime.Stack(trace, true)
			trace_msg := fmt.Sprintf("Stack of %d bytes: %s\n", count, trace)
			log.Error("query crashed: %v,  %v", err, trace_msg)
		}
	}()

	qr := strings.NewReader(sql)
	pl := influxql.NewParser(qr)
	q, err := pl.ParseQuery()
	if err != nil {
		log.Error("parse query fail,",err)
		return meta.Response{},err
	}


	var results meta.Response

	results.Results = []*query.Result{ }
	/***
	SELECT value FROM cpu_load_short WHERE region='us-west';  SELECT * from cpu_load_shortt
	split into two statements 
	***/
	finish := make(chan *meta.Response, 0)
	count := 0

	mergeFlag := false

	over := map[int] map[string] interface{} {}

	for k, stat := range q.Statements {
		switch stat1 := stat.(type) {
		case *influxql.ShowFieldKeysStatement:

				sql := stat.String()
				if strings.Contains(sql,"LIMIT") == false {
					sql = fmt.Sprintf("%s LIMIT %d",sql,MAX_KEY_SIZE)
				}

				if p.Cluster.DbShard.Check(dbname) {
					ret := p.Cluster.FetchAllInfluxs()
					for kk,_ := range ret {
						go ret[kk].Query(dbname, rpname, sql, epoch, chunk, finish,k)
						count = count + 1
					}
				}

				for _, m := range stat1.Sources.Measurements() {

					b := p.Cluster.DbShard.QueryE(dbname, m.Name)

					if b == true {
						mergeFlag = false
						machines := p.Slot.FetchWriteInstances( []byte(m.Name) )
						if len(machines) == 0 {
							log.Warn("measurement does not get slot:", m.Name )
							continue
						}

						c_m := []* meta.Influx{}

						for _,v := range machines {
							ins :=  p.Cluster.FetchReadInflux(v)
							if ins != nil {
								c_m = append(c_m, ins)
							}
						}

						_len := len(c_m)
						_len = rand.Intn(_len)

						machine := c_m[ _len ]

						go machine.Query(dbname, rpname, sql, epoch, chunk, finish,k)
						count = count  + 1
						//log.Info("query on ",machine.Addr," ind:",_len, ", sql: ",stat," db:",dbname)
					} else {
						mergeFlag = true 
						ret := p.Cluster.FetchRandInfluxs()
						for kk,_ := range ret {
							go ret[kk].Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
							count = count + 1
						}						
					}
				}

		case *influxql.ShowMeasurementsStatement:

		case *influxql.ShowTagValuesStatement:

				mergeFlag = true

				for _, m := range stat1.Sources.Measurements() {

					b := p.Cluster.DbShard.QueryE(dbname, m.Name)

					if b == true {
						mergeFlag = false
						machines := p.Slot.FetchWriteInstances( []byte(m.Name) )
						if len(machines) == 0 {
							log.Warn("measurement does not get slot:", m.Name )
							continue
						}

						c_m := []* meta.Influx{}

						for _,v := range machines {
							ins :=  p.Cluster.FetchReadInflux(v)
							if ins != nil {
								c_m = append(c_m, ins)
							}
						}

						_len := len(c_m)
						_len = rand.Intn(_len)

						machine := c_m[ _len ]
						sql := stat.String()
						if strings.Contains(sql,"LIMIT") == false {
							sql = fmt.Sprintf("%s LIMIT %d",sql,MAX_VAL_SIZE)
						}

						go machine.Query(dbname, rpname, sql, epoch, chunk, finish,k)
						count = count  + 1
						//log.Info("query on ",machine.Addr," ind:",_len, ", sql: ",stat," db:",dbname)
					} else {
						mergeFlag = true
						ret := p.Cluster.FetchRandInfluxs()
						for kk,_ := range ret {
							go ret[kk].Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
							count = count + 1
						}						
					}
				}

		case *influxql.ShowTagKeysStatement:

				mergeFlag = true
				for _, m := range stat1.Sources.Measurements() {

					//dbname
					b := p.Cluster.DbShard.QueryE(dbname, m.Name)

					if b == true {
						mergeFlag = false
						machines := p.Slot.FetchWriteInstances( []byte(m.Name) )
						if len(machines) == 0 {
							log.Warn("measurement does not get slot:", m.Name )
							continue
						}

						c_m := []* meta.Influx{}

						for _,v := range machines {
							ins :=  p.Cluster.FetchReadInflux(v)
							if ins != nil {
								c_m = append(c_m, ins)
							}
						}

						_len := len(c_m)
						_len = rand.Intn(_len)

						machine := c_m[ _len ]

						sql := stat.String()
						if strings.Contains(sql,"LIMIT") == false {
							sql = fmt.Sprintf("%s LIMIT %d",sql,MAX_KEY_SIZE)
						}

						go machine.Query(dbname, rpname, sql, epoch, chunk, finish,k)
						count = count  + 1
					} else {
						mergeFlag = true
						ret := p.Cluster.FetchRandInfluxs()
						for kk,_ := range ret {
							go ret[kk].Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
							count = count + 1
						}
					}
					//log.Info("query on ",machine.Addr," ind:",_len, ", sql: ",stat," db:",dbname)
				}

		case *influxql.SelectStatement:

			mergeFlag = false
			if dbname == "_internal" {
				ret := p.Cluster.FetchAllInfluxs()
				for kk,_ := range ret {
					go ret[kk].Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
					count = count + 1
				}
			} else {

				tmp := QueryFilter(stat1)

				over[k] = tmp

				for _, m := range stat1.Sources.Measurements() {

					//dbname
					str,b := p.Cluster.DbShard.QueryFetch(dbname, stat1, m.Name)

					if b == true {
						machines := p.Slot.FetchWriteInstances( str )
						if len(machines) == 0 {
							log.Warn("measurement does not get slot:", m.Name )
							continue
						}

						c_m := []* meta.Influx{}

						for _,v := range machines {
							ins :=  p.Cluster.FetchReadInflux(v)
							if ins != nil {
								c_m = append(c_m, ins)
							}
						}

						_len := len(c_m)
						_len = rand.Intn(_len)

						machine := c_m[ _len ]
						go machine.Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
						count = count  + 1
					} else {
						ret := p.Cluster.FetchRandInfluxs()
						for kk,_ := range ret {
							go ret[kk].Query(dbname, rpname, stat.String(), epoch, chunk, finish,k)
							count = count + 1
						}
					}
				}
			}

		} 		
	}

	//每个sql 有单独的 result
	total := len(q.Statements)
	if mergeFlag == false {
		for i:=0;i< total;i++ {
			t := &query.Result{ StatementID:i,Series: models.Rows{},Messages: []*query.Message{} }
			results.Results = append( results.Results, t  )
		}
	} else {
			t := &query.Result{ StatementID:0,Series: models.Rows{},Messages: []*query.Message{} }
			results.Results = append( results.Results, t  )		
	}


	timeOut := time.After( time.Duration(timeout) * time.Second )
	ansCount := 0
	loop:
	for {
		select {

		case <-timeOut:
			log.Warnf("influxdb query time out!!")
			err = errors.New("query data time out")
			break loop 

		case ret := <-finish:
			
			ind := ret.Index
			for k,_ := range ret.Results {
				p.DataSumm(ret.Results[k].Series, over[ind] )
				results.Results[ind].Series = append( results.Results[ind].Series, ret.Results[k].Series...  )
			}
			ansCount = ansCount + 1
			if ansCount == count {
				break loop
			}					
		}
	}

	return results, nil
}

func (p *Operator) DataSumm(s models.Rows, tmp map[string]interface{} ) models.Rows {

        cc,ok := tmp["flag"]
        cquery := ""
        if ok == true {
            cquery = cc.(string)
        }

        switch cquery{
                case "timeshift":

                        ttt,ok := tmp["time"]
                        if ok == false {
                                return s
                        }
                        tt1 := float64( ttt.( int  ) )
                        for k,_ := range s {
                            for j,_ := range s[k].Values {
                                aa := s[k].Values[j][0].(float64) + tt1
                                s[k].Values[j][0] = aa
                                //fmt.Printf( "******k******%T**%+v\n",s[k].Values[j], s[k].Values[j] )
                            }
                        }
        }
        return s
}

func (p *Operator) WritePoints(points models.Points, 
					dbname string, 
					rpname string, 
					precision string )  error {


	if p.Slot == nil {
		return errors.New("slot fail!")
	}

	modelP  := map[string] []models.Point{}

	//instanceCount := 0

	for _,v := range points {
		str := p.Cluster.DbShard.Fetch(dbname,v)
		machines := p.Slot.FetchWriteInstances( str )
		if len(machines) == 0 {
			log.Warn("measurement does not get slot:", string( v.Name() ) )
		}

		m_machines := p.Migra.FetchWriteInstances( str )
		if len(m_machines) != 0 {
			for k,_ := range m_machines {
				machines = append( machines, m_machines[k] )
				//log.Debug("**migra***%v\n",m_machines[k])
			}
		}

		for _,instance := range machines {
			_,ok := modelP[ instance ]
			if ok == false {
				modelP[ instance ] = []models.Point{}
				//instanceCount = instanceCount + 1
			}
			modelP[ instance ] = append( modelP[ instance ], v )
		}
	}

	//instanceFinish := make(chan error, instanceCount+1)

	for k,v := range modelP {
		ins := p.Cluster.FetchWriteInflux(k)
		if ins != nil {
			//go ins.WritePoints(v,instanceFinish, dbname, rpname, precision)
			ins.PutPoints(v, dbname,rpname)
			//instanceFinish <- nil
		} else {
			//instanceCount = instanceCount - 1
			log.Error("catch influxdb not exist:",k)
		}
	}
	return nil
}

	/***finishCount := 0
	timeOut := time.After( p.WriteTimeOut )
	//loop:
	for {
		select {
		case <-timeOut:
			log.Warnf("influxdb write time out!!")
			return errors.New("write data time out")

		case err := <-instanceFinish:
			if err != nil {
				log.Error("write err",err)
			} else {
				finishCount =  finishCount + 1
			}

			if finishCount == instanceCount {
				log.Debug("finish all influxdb write: ",finishCount,instanceCount)
				return nil
			} else {
				//return errors.New("some write data action fail")
			}
		}
	}***/







