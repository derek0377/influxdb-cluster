package meta


import (
	"github.com/infP/utils"
	"fmt"
	"reflect"
	log "github.com/Sirupsen/logrus"
	"sync"
)

var (
	kClusterSlots = uint16(16384)
	_ = fmt.Sprintf("")
)

type Slot struct {
	meta map[uint16] []string
	sy    sync.Mutex
}


func (a *Slot) Init() {
	a.sy.Lock()
	defer a.sy.Unlock()
	a.meta =  map[uint16] []string {}
}


func (a *Slot) Append(slot uint16, instance []string) {
	v,ok := a.meta[slot]
	if ok == false {
		a.meta[slot] = []string{}
	}

	v = append( v, instance... )
	a.meta[slot] = v
	log.Info( fmt.Sprintf("slot append****instance:%v****slot:%v**all:%+v",instance, slot,a.meta[slot]) )

}


func (a *Slot) SetSlot(tmp map[uint16] []string) {
	a.sy.Lock()
	defer a.sy.Unlock()

	count := 0
	for k,v := range tmp {
		cv,ok := a.meta[k]
		if ok == false {
			a.meta[k] = v
			log.Debug("change slot:",k," list:",v)
		}
		result := reflect.DeepEqual(v, cv)
		if result == false {
			log.Debug("change slot:",k," list:",v)
			a.meta[k] = v
			count = count  + 1
		}
	}
	if count != 0 {
		log.Info("**set slot***total*",count)
	}
}

func (s *Slot) FetchWriteInstances(measurement []byte) []string {
	slot := s.FetchSlot(measurement)
	//log.Debug("fetch slot:",string(measurement),slot)
	if s.meta == nil {
		return []string{}
	}

	s.sy.Lock()
	defer s.sy.Unlock()

	list,ok := s.meta[slot]
	if ok {
		return list
	} else {
		return []string{}
	}
}

func (sl *Slot) FetchSlot(key []byte) uint16 {
    var s, e int
    key_len := len(key)
    for s = 0; s < key_len; s++ {
		if key[s] == '{' {
		    break
		}
    }

    if s == key_len {
		return utils.Crc16(key) & (kClusterSlots-1)
    }

    for e = s+1; e < key_len; e++ {
		if key[e] == '}' {
		    break
		}
    }

    if e == key_len || e == s+1 {
		return utils.Crc16(key) & (kClusterSlots-1)
    }

    return utils.Crc16(key[s+1:e]) & (kClusterSlots-1)
}