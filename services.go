package etcdservices

import (
	"time"
	"errors"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	//"log"
)

const (
	HEARTBEAT_DURATION = 5
	KEY_TTL = 16
)

const (
	ALL = 0
	New = iota
	Down = iota
	Update = iota
)

var (
ErrInvalidParams = errors.New("invalid parameters!")
ErrHeartBeatTime = errors.New("invalid heart beat time!")	
ErrKeyTTLTime = errors.New("invalid ttl time!")
)


type EtcdServices struct {
	heartBeat uint64
	client *etcd.Client
	keyTTL uint64
}

// Create a new etc services
func NewEtcdServices(c *etcd.Client) *EtcdServices {
	es := &EtcdServices{HEARTBEAT_DURATION , c, KEY_TTL}
	return es
}


// Set heart beat as second
// if t is invalid, return ErrHeartBeatTime
func (this *EtcdServices) SetHeartBeat(t uint64) error {
	if t < 2 || this.keyTTL <= t {
		return ErrHeartBeatTime
	}
	this.heartBeat = t
	return nil
}

// Set ttl of key ( must greater than heaert beat )
// eg : heartbeart = 3, keyttl = 9, 
// suggestion : keyTTL is three times larger than heartbeart
func (this *EtcdServices) SetKeyTTL(t uint64) error {
	if t <= this.heartBeat {
		return ErrKeyTTLTime
	}
	this.keyTTL = t
	return nil
}


func ErrorKeyExist(err error) bool {
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && etcdErr.ErrorCode == 105
}

func ErrorNotFound(err error) bool {
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && etcdErr.ErrorCode == 100
}

// Get all nodes which service name matched
func (this *EtcdServices)Get(service string) (*etcd.Response, error) {
	return this.client.Get("/services/"+service, false, true)
}

// Register a node to the service,  
// service : service name , eg: rediscluster
// nodename : node name, eg: redis-IP-ProcessID
// node value : 
// stop : stop channel
// return *etc.Response and error
func (this *EtcdServices) Register(service string, nodename string, nodevalue string, stop chan struct{}) (registered *etcd.Response, err error) {
	if len(nodevalue) == 0 {
		return nil, ErrInvalidParams
	}

	key := fmt.Sprintf("/services/%s/%s", service, nodename)

	go func() {
		ticker := time.NewTicker((time.Duration)(this.heartBeat - 1) * time.Second)
		registered, err = this.client.Set(key, nodevalue, this.keyTTL)
		if err != nil {
			return
		}
		for {
			select {
			case <-stop:
				_, err = this.client.Delete(key, false)
				ticker.Stop()
				return
			case <-ticker.C:
				_, err = this.client.Update(key, nodevalue, this.heartBeat)
				for err != nil {
					//errEtcd := err.(*etcd.EtcdError)
					//logger.Printf("register %v fail : %v [clusters: %v]", service, errEtcd.Message, this.client.GetCluster())
					time.Sleep(2 * time.Second)
					_, err = this.client.Set(key, nodevalue, this.heartBeat)
				}
			}
		}
	}()

	return
}


// Subscribe : watch the status change of all nodes of the service
func (this *EtcdServices) Subscribe(service string) (<-chan *etcd.Response, <-chan error) {
	stop := make(chan bool)
	responseChan := make(chan *etcd.Response)
	errorsChan:= make(chan error)
	go func() {
		_, err := this.client.Watch("/services/"+service, 0, true, responseChan, stop)
		if err != nil {
			errorsChan <- err
			close(errorsChan)
			close(stop)
			return
		}
	}()
	return responseChan, errorsChan
}


func (this *EtcdServices)subscribetype(service string, tt int) (<-chan *etcd.Response, <-chan error) {
	responseChan := make(chan *etcd.Response)
	responses, errors := this.Subscribe(service)
	go func() {
		for response := range responses {
			switch tt {
				case New:
					if response.Action == "create" || response.Action == "new" {
						responseChan <- response
					}
				case Down:
					if response.Action == "expire" || response.Action == "delete" {
						responseChan <- response
					}
				case Update:
					if response.Action == "update" {
						responseChan <- response
					}
			}
		}
	}()
	return responseChan, errors
}


// SubscribeDown : watch the status is expire or delete of all nodes
func (this *EtcdServices)SubscribeDown(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, Down)
}


// SubscribeDown : watch the status is create or new of all nodes
func (this *EtcdServices)SubscribeNew(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, New)
}

// SubscribeDown : watch the status is update of all nodes
func (this *EtcdServices)SubscribeUpdate(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, Update)
}












