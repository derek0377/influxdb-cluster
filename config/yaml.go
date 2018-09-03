package config

import (
    "gopkg.in/yaml.v2"
    "io/ioutil"
    _ "fmt"
    log "github.com/Sirupsen/logrus"
    "time"
)


type Conf struct {
    Zk          string   `yaml:"zk"`
    Zk_path     string   `yaml:"zk_path"`
    Port        string   `yaml:"port"`
    Write_timeout int    `yaml:write_timeout`

    Udp_path    string   `yaml:udp_path`
    Ser_status    string   `yaml:"ser_status"`
    Migra_slot  string   `yaml:"migra_slot"`

    Persi_queue string   `yaml:persi_queue`

    Write_worker int     `yaml:write_worker`

    Log_level  string    `yaml:"log_level"`

    Db_shard string `yaml:"db_shard"`

    //poc code
    Trigger   string  `yaml:"trigger"`
    
    Write_timeout_dur  time.Duration
}


func (c *Conf) Get_conf( path string ) error {

    yamlFile, err := ioutil.ReadFile(path)
    if err != nil {
        return err
    }

    err = yaml.Unmarshal(yamlFile, c)
    if err != nil {
        return err
    }

    c.Write_timeout_dur = time.Second * time.Duration(c.Write_timeout)

    m := map[string] log.Level {
        "info": log.InfoLevel,
        "debug": log.DebugLevel,
    }
    d,ok := m[ c.Log_level  ]
    if ok {
        log.SetLevel(d)
    }

    log.Info("fetch config: zk is:",c.Zk,", server port is:",c.Port,",  write_timeout:",c.Write_timeout_dur," udp: ",c.Udp_path)
    return nil
}