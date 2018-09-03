package meta

import (
	"fmt"
	"github.com/influxdata/influxdb/models"
	log "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxql"
	"strings"
	"bytes"
	"sort"
	"reflect"
)

var (
	_ = fmt.Sprintf("")
)

type  DbShard struct {
	//支持单个measurement+ 多tag的sharding
	db_tag map[string] []string
	db_s map[string] string
}


func (i *DbShard) Init() {
	i.db_tag  = map[string] []string{}
	i.db_s = map[string] string{}
}

func (i *DbShard) Check(db string) bool {
	_,ok := i.db_s[ db  ]
	return ok
}

func FetchEqual(conda interface{}, tmp map[string]string ) {
	types := reflect.TypeOf(conda).String()

	if types == "influxql.BinaryExpr"  {
		cond := conda.(influxql.BinaryExpr)
        switch cond.Op {
            case influxql.EQ:
                key := cond.LHS.String()
                value := strings.Trim(cond.RHS.String(),"'")
            	tmp[key] = value

            default:
            	FetchEqual(cond.LHS,tmp)
            	FetchEqual(cond.RHS,tmp)
        } 		
	}
	if types == "*influxql.BinaryExpr" {
		cond := conda.(*influxql.BinaryExpr)
        switch cond.Op {
            case influxql.EQ:
                key := cond.LHS.String()
                value := strings.Trim(cond.RHS.String(),"'")
            	tmp[key] = value

            default:
            	FetchEqual(cond.LHS,tmp)
            	FetchEqual(cond.RHS,tmp)
        }		
	}

	if types == "influxql.ParenExpr"   {
		cond1 := conda.(influxql.ParenExpr)
		cond := cond1.Expr
		FetchEqual(cond,tmp)
	}
	
    if types == "*influxql.ParenExpr"  {
            cond1 := conda.(*influxql.ParenExpr)
            cond := cond1.Expr
            FetchEqual(cond,tmp)
    }


}

func (i *DbShard) QueryE(db string,m string) bool {
    pn,ok := i.db_s[ db  ]
    if ok == false {
        return true
    }
    if pn != "*" &&  m != pn {
    	return true
    }
    return false
}

func (i *DbShard) QueryFetch(db string,stat1 *influxql.SelectStatement,m string) ( []byte,bool ) {

    pn,ok := i.db_s[ db  ]
    if ok == false  {
            return []byte(m),true
    }

    if pn != "*" && m != pn  {
            return []byte(m),true
    }

	zbuf := bytes.NewBuffer(nil)
	zbuf.Write( []byte(m) )

	tmp := map[string]string{}

	FetchEqual(stat1.Condition, tmp)

	flag := true

	tagList := i.db_tag[ db ]
    for k,_ := range tagList {

    	data,ok := tmp[ tagList[k] ]
    	if ok {
    		zbuf.Write( []byte(data) )
    	} else {
    		flag = false
    	}
    }

    if flag {
    	return zbuf.Bytes(),true
    } else {
    	return []byte{},false
    }
}

func (i *DbShard) Fetch(db string, point models.Point) []byte {
	m,ok := i.db_s[ db  ]
	pn := point.Name()

    if ok == false  {
            return pn
    }

    if m != "*" && m != string(pn)  {
            return pn
    }

	zbuf := bytes.NewBuffer(nil)
	zbuf.Write( pn )

	t := point.Tags()
	tagList := i.db_tag[ db ]

	for _,v1 := range t {
		for k,_ := range tagList {
			if string(v1.Key) == tagList[k] {
				zbuf.Write( v1.Value )
			}
		}		
	}

	return zbuf.Bytes()
}


func (i *DbShard) Set(coming []string) {

	db_tag  := map[string] []string{}
	db_s := map[string] string{}

	for _,v := range coming {

		if len(v) <= 2 {
			continue
		}

		if v[0:1] == "#" {
			continue
		}

		ret := strings.Split(v," ")
		if len(ret) < 2 {
			continue
		}

		keyList := strings.Split(ret[1],":")

		if len(keyList) != 2 {
			continue
		}

		m := keyList[0]
		tagListstr := keyList[1]
		tagList := strings.Split(tagListstr,",")

		sort.Strings( tagList )

		db_s[ ret[0] ] = m
		db_tag[ ret[0] ] = tagList
	}

	i.db_tag = db_tag
	i.db_s = db_s
	log.Info("db sharding strategy:  measurement list:", i.db_s," tag list:",i.db_tag )
}




