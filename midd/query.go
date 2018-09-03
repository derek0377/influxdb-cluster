package midd

import (
	//"github.com/influxdata/influxdb/models"
	//"github.com/infP/meta"
	//log "github.com/Sirupsen/logrus"
	"fmt"
	//"errors"
	//"time"
	//"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	//"github.com/abrander/influxdb/influxql"
	//"strings"
	//"math/rand"
	//"runtime"
	"reflect"
	"time"
)

var (
	_ = fmt.Sprintf("")
)


func shTime(in string, h int) {

}

// now - 6h time shift 22h is   > now - 28h and < now - 22h
func findTime(conda interface{}, tmp map[string] time.Duration, h int ) {
	types := reflect.TypeOf(conda).String()

	if types == "influxql.BinaryExpr"  {
		cond := conda.(influxql.BinaryExpr)
        key := cond.LHS.String()

        if key == "time" {
                dd := cond.RHS.(*influxql.BinaryExpr)
                switch  dd.RHS.(type) {
                	case *influxql.DurationLiteral:
                		r1 := dd.RHS.(*influxql.DurationLiteral)
                		v := r1.Val
                		r1.Val = v + time.Duration( h) * time.Hour 
                		tmp[ "add" ] = time.Duration( h) * time.Hour
                }
                
        } else {
            	findTime(cond.LHS,tmp,h)
            	findTime(cond.RHS,tmp,h)
        } 		
	}

	if types == "*influxql.BinaryExpr" {

		cond := conda.(*influxql.BinaryExpr)

        key := cond.LHS.String()

        if key == "time" {
                dd := cond.RHS.(*influxql.BinaryExpr)
                switch  dd.RHS.(type) {
                	case *influxql.DurationLiteral:
                		r1 := dd.RHS.(*influxql.DurationLiteral)
                		v := r1.Val
                		r1.Val = v + time.Duration( h) * time.Hour 
                		tmp[ "add" ] = time.Duration( h) * time.Hour
                }
                
        } else {
            	findTime(cond.LHS,tmp,h)
            	findTime(cond.RHS,tmp,h)
        } 			
	}

	if types == "influxql.ParenExpr"   {
		cond1 := conda.(influxql.ParenExpr)
		cond := cond1.Expr
		findTime(cond,tmp,h)
	}
	
    if types == "*influxql.ParenExpr"  {
        cond1 := conda.(*influxql.ParenExpr)
        cond := cond1.Expr
        findTime(cond,tmp,h)
    }
}


func timeShift(stat1 *influxql.SelectStatement,args [] influxql.Expr, k int) int{
	//con := stat1.Condition

	//args len must 2
	if len(args) != 2 {
		return 0
	}

	stat1.Fields[k].Expr = args[0]
	h := args[1].(*influxql.IntegerLiteral).Val

	//fmt.Printf("******args***%v*%T*****%v\n",con,con, stat1)	
	tmp := map[string]time.Duration{}
	findTime(stat1.Condition, tmp, int(h))
	ti,ok := tmp["add"]
	if ok == true {
		sql := fmt.Sprintf("%s AND time < now() - %s",stat1.Condition.String(), ti.String())
		stat1.Condition,_ = influxql.ParseExpr(sql)
	}
	return int(h)*3600*1000
}


func QueryFilter(stat1 *influxql.SelectStatement) map[string]interface{} {
	fields := stat1.Fields

	tmp := map[string]interface{} {}

	for k,_ := range fields {
		e := fields[k].Expr

		switch d := e.(type) {
			case *influxql.Call:
				f := d.Name
				//Args := e.Args
				switch f {
					case "timeshift":
						t := timeShift(stat1,d.Args,k)
						tmp["flag"] = "timeshift"
						tmp["time"] = t
						break
				}
		}
	}
	return tmp
}
