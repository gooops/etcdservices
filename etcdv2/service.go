package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
)

func main() {
	var hosts = []string{"http://10.10.103.11:2379"}
	var client, err = etcd.New(etcd.Config{Endpoints: hosts})
	if err != nil {
		log.Fatalln(err)
	}
	var etcservices = NewEtcdServices(client)

	fmt.Println("实例化", etcservices)
	go func() {
		// 监听
		wc, err := etcservices.SubscribeUpdate("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("服务更新", a.Node)
			case e := <-err:
				fmt.Println(e)
			}
		}
	}()

	go func() {
		// 监听
		wc, err := etcservices.SubscribeNew("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("新注册服务", a)
			case e := <-err:
				fmt.Println(e)
			}
		}
	}()

	go func() {
		// 监听
		wc, err := etcservices.SubscribeDown("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("服务下线", a)
			case e := <-err:
				fmt.Println(e)
			}
		}
	}()

	stop := make(chan struct{})
	// 注册服务
	fmt.Println("注册服务")
	service, err := etcservices.Register("kkkk", "127.0.0.1", "1111111", stop)
	fmt.Println(service, err)
	r, err := etcservices.Get("kkkk")
	fmt.Println("查询服务", r.Node.Nodes, err)
	time.Sleep(time.Second * 60)
	fmt.Println("停止服务")
	stop <- struct{}{}
	select {}
}

const (
	HEARTBEAT_DURATION = 5
	KEY_TTL            = 16
)

const (
	ALL    = 0
	New    = iota
	Down   = iota
	Update = iota
)

var (
	ErrInvalidParams = errors.New("invalid parameters!")
	ErrHeartBeatTime = errors.New("invalid heart beat time!")
	ErrKeyTTLTime    = errors.New("invalid ttl time!")
)

type EtcdServices struct {
	heartBeat uint64
	kapi      etcd.KeysAPI

	keyTTL uint64
}

// Create a new etc services
func NewEtcdServices(c etcd.Client) *EtcdServices {
	es := &EtcdServices{HEARTBEAT_DURATION, etcd.NewKeysAPI(c), KEY_TTL}
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

// Get all nodes which service name matched
func (this *EtcdServices) Get(service string) (*etcd.Response, error) {
	return this.kapi.Get(context.TODO(), "/services/"+service, &etcd.GetOptions{Recursive: true})
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
	registered, err = this.kapi.Set(context.Background(), key, nodevalue, &etcd.SetOptions{TTL: time.Duration(this.heartBeat) * time.Second, PrevExist: "false"})
	if err != nil {
		return
	}
	go func() {
		ticker := time.NewTicker((time.Duration)(this.heartBeat-1) * time.Second)
		_, err = this.kapi.Set(context.Background(), key, nodevalue, &etcd.SetOptions{TTL: time.Duration(this.heartBeat) * time.Second})
		for {
			select {
			case <-stop:
				_, err = this.kapi.Delete(context.TODO(), key, nil)
				ticker.Stop()
				return
			case <-ticker.C:
				_, err = this.kapi.Set(context.Background(), key, nodevalue, &etcd.SetOptions{TTL: time.Duration(this.heartBeat) * time.Second, PrevExist: "true"})
				for err != nil {
					//errEtcd := err.(*etcd.EtcdError)
					//logger.Printf("register %v fail : %v [clusters: %v]", service, errEtcd.Message, this.kapi.GetCluster())
					time.Sleep(2 * time.Second)
					_, err = this.kapi.Set(context.Background(), key, nodevalue, &etcd.SetOptions{TTL: time.Duration(this.heartBeat) * time.Second, PrevExist: "true"})
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
	errorsChan := make(chan error)
	watcher := this.kapi.Watcher("/services/"+service, &etcd.WatcherOptions{
		Recursive: true,
	})
	go func() {
		for {
			resp, err := watcher.Next(context.Background())
			if err != nil {
				errorsChan <- err
				close(errorsChan)
				close(stop)
				return
			}
			responseChan <- resp
		}
	}()
	return responseChan, errorsChan
}

func (this *EtcdServices) subscribetype(service string, tt int) (<-chan *etcd.Response, <-chan error) {
	responseChan := make(chan *etcd.Response)
	responses, errors := this.Subscribe(service)
	go func() {
		for response := range responses {
			switch tt {
			case New:
				// &etcd.SetOptions{PrevExist: "false"} 事件为 create
				if response.Action == "create" {
					responseChan <- response
				}
			case Down:
				if response.Action == "expire" || response.Action == "delete" {
					responseChan <- response
				}
			case Update:
				// &etcd.SetOptions{PrevExist: "true"} 事件为 update
				if response.Action == "update" {
					responseChan <- response
				}
			}
		}
	}()
	return responseChan, errors
}

// SubscribeDown : watch the status is expire or delete of all nodes
func (this *EtcdServices) SubscribeDown(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, Down)
}

// SubscribeDown : watch the status is create or new of all nodes
func (this *EtcdServices) SubscribeNew(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, New)
}

// SubscribeDown : watch the status is update of all nodes
func (this *EtcdServices) SubscribeUpdate(service string) (<-chan *etcd.Response, <-chan error) {
	return this.subscribetype(service, Update)
}
