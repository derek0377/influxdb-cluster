package http

import (
	stdContext "context"
	log "github.com/Sirupsen/logrus"
	"github.com/infP/config"
	"github.com/infP/midd"
	"github.com/infP/meta"
	"gopkg.in/kataras/iris.v6"
	"gopkg.in/kataras/iris.v6/adaptors/cors"
	"gopkg.in/kataras/iris.v6/adaptors/httprouter"
	"time"
	"fmt"
)

type ApiService struct {
	App         *iris.Framework
	shutdownCh  chan chan error
	Conf        *config.Conf

	Cluster     *meta.Cluster

	Operator *midd.Operator
}

func (s *ApiService) Run() {

	log.Info("server run,port:",s.Conf.Port)
	s.App.Listen( fmt.Sprintf(":%s",s.Conf.Port))
}



func NewApiService(conf *config.Conf) (*ApiService, error) {

	app := iris.New(iris.Configuration{Gzip: true, Charset: "UTF-8"})

	app.Adapt(
		iris.DevLogger(),
		httprouter.New(),
		cors.New(cors.Options{AllowedOrigins: []string{"*"}}),
	)

	app.Config.MaxHeaderBytes = 10 << 20

	service := &ApiService{
		App:        app,
		shutdownCh: make(chan chan error),
		Conf: conf,
		Operator: &midd.Operator{},

		Cluster: &meta.Cluster{},
	}

	service.Cluster.Init(conf)

	
	service.Operator.SetCluster(service.Cluster)

	service.Routes(app)

	go service.waitShutDown()

	return service, nil
}

func (s *ApiService) waitShutDown() {
	for {
		select {
		case rCh := <-s.shutdownCh:
			timeout := 5 * time.Second
			ctx, cancel := stdContext.WithTimeout(stdContext.Background(), timeout)
			defer cancel()
			err := s.App.Shutdown(ctx)
			rCh <- err
		}
	}
}

func (s *ApiService) Shutdown() error {
	rCh := make(chan error)
	s.shutdownCh <- rCh
	return <-rCh
}
