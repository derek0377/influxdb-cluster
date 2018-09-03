package http

import (
	"gopkg.in/kataras/iris.v6"
	"gopkg.in/kataras/iris.v6/middleware/pprof"
)

var (
	ROUTES_INFLUXDB_V1 = "/api/influxdb/v1"
)

func (s *ApiService) Routes(app *iris.Framework) {


	app.Get("/debug/pprof/*action", pprof.New())

	app.Get("/", s.index)

	app.Post("/write", s.handleInfluxDbWrite)

	//influxdbV1 := app.Party(ROUTES_INFLUXDB_V1)


	app.Get("/query", s.handleInfluxDbRead)
	app.Post("/query", s.handleInfluxDbRead)


	app.Post("/api/v1/prom/write", s.servePromWrite)

	app.Post("/api/v1/prom/read", s.servePromRead)
}
