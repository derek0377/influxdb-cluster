package meta

import (
	"strconv"
	"fmt"
	log "github.com/Sirupsen/logrus"
	//reuse "github.com/jbenet/go-reuseport"
    "golang.org/x/sys/unix"
    "net"
    "time"
    "github.com/influxdata/influxdb/models"
    "errors"
)

const (
	// MaxUDPPayload is largest payload size the UDP service will accept.
	MaxUDPPayload = 64 * 1024
	//548
)


//Listener
type Udp struct {
	addr string
	port int
	db  string
	worker int
	inst  map[int] chan bool

	Cluster *Cluster
	Slot    *Slot
	Migra   *Slot

}

func (s *Udp) Db() string {
	return s.db
}

func (c *Udp) Worker() string {
	return fmt.Sprintf("%d",c.worker)
}

func (s *Udp) Init(port string, db string,worker string,c *Cluster) {
	s.db = db
	s.addr = fmt.Sprintf("0.0.0.0:%s",port)

	s.port,_ = strconv.Atoi( port )
	s.worker,_ = strconv.Atoi( worker )
	s.inst = map[int] chan bool{}

	s.Cluster = c
	s.Slot = c.Slot
	s.Migra = c.Migra

	for k := 0; k < s.worker; k ++ {
		stop := make(chan bool,2)
		go s.Open(stop)
		s.inst[  k  ] = stop
	}
}



func (s *Udp) initSocket() (int, error) {

    fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, unix.IPPROTO_UDP)

	err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	if err != nil {
        log.Error("socket resue err:",err)
        return 0, err
	}

	if err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
        log.Error("socket resue err:",err)
        return 0,err
	}

    sec := 20
 	var l unix.Linger
	if sec >= 0 {
		l.Onoff = 1
		l.Linger = int32(sec)
	} else {
		l.Onoff = 0
		l.Linger = 0
	}

    err = unix.SetsockoptLinger(fd, unix.SOL_SOCKET, unix.SO_LINGER, &l)
    if err != nil {
       log.Error("socket set Linger fail;",err)
    }

    t := 5 * time.Second
	tv := unix.NsecToTimeval(t.Nanoseconds())
	err = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
    if err != nil {
       log.Error("socket set time out fail;",err)
    }


	netadd,err := net.ResolveUDPAddr("udp", "0.0.0.0:12346") 

	ip4 := netadd.IP.To4()
	var buf [4]byte
	copy(buf[:], ip4)
	n := unix.SockaddrInet4{ Addr: buf,Port: s.port }

	if err = unix.Bind(fd, &n); err != nil {
		log.Error("bind socket fail:",err)
	}

	return fd,nil
}


func (p *Udp) Write(dd []byte) error {

	points, parseError := models.ParsePointsWithPrecision(dd, time.Now().UTC(), "")

	if parseError != nil {
		log.Error("udp format points fail:",parseError)
		return parseError
	} 

	if p.Slot == nil {
		return errors.New("slot fail!")
	}

	modelP  := map[string] []models.Point{}

	instanceCount := 0

	//log.Info("******points*******",points)
	//log.Info("*****len*points*******",len(points)

	for _,v := range points {

		str := p.Cluster.DbShard.Fetch(p.db,v)

		machines := p.Slot.FetchWriteInstances( str )
		if len(machines) == 0 {
			log.Warn("measurement does not get slot:", string( v.Name() ) )
		}

		m_machines := p.Migra.FetchWriteInstances( str)
		if len(m_machines) != 0 {
			for k,_ := range m_machines {
				machines = append( machines, m_machines[k] )
			}
		}

		for _,instance := range machines {
			_,ok := modelP[ instance ]
			if ok == false {
				modelP[ instance ] = []models.Point{}
				instanceCount = instanceCount + 1
			}
			modelP[ instance ] = append( modelP[ instance ], v )
		}
	}


	for k,v := range modelP {
		ins := p.Cluster.FetchWriteInflux(k)
		if ins != nil {
			ins.PutPoints(v, p.db,"")
			//ins.PutPointsBytes(v,p.db)
		}
	}

	return nil
}


// Open starts the service.
func (s *Udp) Open(stop chan bool) {

	fd, err := s.initSocket()
	if err != nil {
		log.Error("open socket fail:",err)
		return
	}

	buf := make([]byte, MaxUDPPayload)

	log.Info("open udp server successfully,port is:",s.port," fd is:",fd)
	loop:
	for {
		select {
		case <-stop:
			log.Info("close udp handle")
			//handle.Close()
			unix.Close(fd)
			break loop
		default:
			n,_,err := unix.Recvfrom(fd,buf,0)
			if err == nil {
				dd := make([]byte, n)
				copy(dd[:], buf)
				s.Write(dd)
			} 
		}
	}	

}

func (s *Udp) Close()  {
	fmt.Printf("!!!close\n")
	for k,_ := range s.inst {
		s.inst[ k ] <- true
	}
}

