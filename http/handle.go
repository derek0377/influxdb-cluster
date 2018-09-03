package http

import (
	"bytes"
	"compress/gzip"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/kataras/iris.v6"
	"time"
	"fmt"
	"strings"
	"strconv"
	//"github.com/influxdata/influxdb/influxql"
	//"github.com/infP/midd"
	"github.com/influxdata/influxdb/models"
	"github.com/golang/snappy"
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/infP/utils"
)


var (
	_ = fmt.Sprintf("")
)

func errJsonResp(ctx *iris.Context, status int, err string) {
	log.Errorf("errJsonResp:%v",err)

	ctx.JSON(status, map[string]string{"error": err})
}

func (s *ApiService) index(ctx *iris.Context) {

	log.Debug("entry main page")
	ctx.HTML(iris.StatusOK, "ok")

}

func (s *ApiService) servePromRead(ctx *iris.Context) { 

	var body = ctx.Request.Body
	defer ctx.Request.Body.Close()

	bodyBuf := bytes.NewBuffer(nil)
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, "problem reading request body")
		return
	}

	reqBuf, err := snappy.Decode(nil, bodyBuf.Bytes())
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	var req remote.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	// Query the DB and create a ReadResponse for Prometheus
	db := ctx.FormValue("db")
	rp := ctx.FormValue("rp")

	q, err := utils.ReadRequestToInfluxQLQuery( &req, db, rp )
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	epoch := ""
	chunk := ""
	timeout := 60

	log.Debugf("read:%v,%v,%v,%v,%v,%v",db,epoch,chunk,rp,q,timeout)

	resp := &remote.ReadResponse{
		Results: []*remote.QueryResult{{}},
	}

	rr,err := s.Operator.Query(db, rp, q.String(), epoch, chunk,timeout)

	for _,r := range rr.Results {

		// read the series data and convert into Prometheus samples
		for _, s := range r.Series {
			ts := &remote.TimeSeries{
				Labels: utils.TagsToLabelPairs(s.Tags),
			}

			for _, v := range s.Values {
				t, ok := v[0].(time.Time)
				if !ok {
					errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
					return
				}
				val, ok := v[1].(float64)
				if !ok {
					errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
					return
				}
				timestamp := t.UnixNano() / int64(time.Millisecond) / int64(time.Nanosecond)
				ts.Samples = append(ts.Samples, &remote.Sample{
					TimestampMs: timestamp,
					Value:       val,
				})
			}

			resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
		}
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	w := ctx.ResponseWriter

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)

	if _, err := w.Write(compressed); err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}
		
}



func (s *ApiService) servePromWrite(ctx *iris.Context) {
	var body = ctx.Request.Body
	defer ctx.Request.Body.Close()
	dbname := ctx.FormValue("db")


	bodyBuf := bytes.NewBuffer(nil)
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, "problem reading request body")
		return
	}

	reqBuf, err := snappy.Decode(nil, bodyBuf.Bytes())
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var req remote.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
		return
	}

	points, err := utils.WriteRequestToPoints(&req)
	if err != nil {
		if err != utils.ErrNaNDropped {
			errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
			return
		}
	}

	err = s.Operator.WritePoints(points, dbname, "", "")

	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
	} else {
		ctx.JSON(iris.StatusNoContent, nil)
	}

}

func (s *ApiService) handleInfluxDbWrite(ctx *iris.Context) {

	var body = ctx.Request.Body
	defer ctx.Request.Body.Close()
	if ctx.Request.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(ctx.Request.Body)
		if err != nil {
			errJsonResp(ctx, iris.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}

	bodyBuf := bytes.NewBuffer(nil)
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, "problem reading request body")
		return
	}

	consistency := ctx.FormValue("consistency")
	dbname := ctx.FormValue("db")
	rpname := ctx.FormValue("rp")   //rp null means use default rp
	precision := ctx.FormValue("precision")


	points, parseError := models.ParsePointsWithPrecision(bodyBuf.Bytes(), time.Now().UTC(), consistency)

	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			ctx.JSON(iris.StatusOK, "")
			return
		}
		errJsonResp(ctx, iris.StatusInternalServerError, "parsed point fail")
		return
	}


	//log.Debugf("dbname: %v, rpname: %v, precision: %v", dbname, rpname, precision)

	err = s.Operator.WritePoints(points, dbname, rpname, precision)

	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
	} else {
		ctx.JSON(iris.StatusNoContent, nil)
	}	
	
}


func (s *ApiService) handleInfluxDbRead(ctx *iris.Context) {

	dbname := strings.TrimSpace(ctx.FormValue("db"))
	epoch := strings.TrimSpace(ctx.FormValue("epoch"))
	chunk := strings.TrimSpace(ctx.FormValue("chunk"))
	rpname := strings.TrimSpace(ctx.FormValue("rp"))
	q := strings.TrimSpace(ctx.FormValue("q"))
	timeoutstr := strings.TrimSpace(ctx.FormValue("timeout"))

	start := time.Now()
	var timeout int
	var err error
	if timeoutstr == "" {
		timeout = 10
	} else {
		timeout, err = strconv.Atoi(timeoutstr)
		if err != nil {
			errJsonResp(ctx, iris.StatusBadRequest, err.Error())
			return
		}
	}

	log.Debugf("at time:%v read:%v,%v,%v,%v,%v,%v",start,dbname,epoch,chunk,rpname,q,timeout)

	r,err := s.Operator.Query(dbname, rpname, q, epoch, chunk,timeout)

	if err != nil {
		errJsonResp(ctx, iris.StatusInternalServerError, fmt.Sprintf("failed to write data: %v", err))
	} else {
		ctx.JSON(iris.StatusOK, r)	
	}	
}



