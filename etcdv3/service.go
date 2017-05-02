package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

// 命令行调试
/*
export ETCDCTL_API=3
etcdctl get --prefix /services
etcdctl watch --prefix /services
*/

func main() {
	var hosts = []string{"10.10.103.11:2379"}
	dialTimeout := 5 * time.Second
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   hosts,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	var etcservices = NewEtcdServices(client)
	fmt.Println(etcservices)
	go func() {
		// 监听
		wc := etcservices.SubscribeUpdate("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("服务更新", a)
				for _, e := range a.Events {
					fmt.Println(e.Kv.String())
				}
			}
		}
	}()

	go func() {
		// 监听
		wc := etcservices.SubscribeNew("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("新注册服务", a)
			}
		}
	}()

	go func() {
		// 监听
		wc := etcservices.SubscribeDown("kkkk")
		for {
			select {
			case a := <-wc:
				fmt.Println("服务下线", a)
			}
		}
	}()

	stop := make(chan struct{})
	// 注册服务
	fmt.Println("注册服务")
	service, err := etcservices.Register("kkkk", "127.0.0.1", "1111111", stop)
	fmt.Println(service, err)
	time.Sleep(10 * time.Second)
	r, err := etcservices.Get("kkkk")
	fmt.Println(r.Count, r.Header.String(), r.Kvs, err)
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
	heartBeat int64
	client    *clientv3.Client
	keyTTL    int64
}

// Create a new etc services
func NewEtcdServices(c *clientv3.Client) *EtcdServices {
	es := &EtcdServices{HEARTBEAT_DURATION, c, KEY_TTL}
	return es
}

// Set heart beat as second
// if t is invalid, return ErrHeartBeatTime
func (this *EtcdServices) SetHeartBeat(t int64) error {
	if t < 2 || this.keyTTL <= t {
		return ErrHeartBeatTime
	}
	this.heartBeat = t
	return nil
}

// Set ttl of key ( must greater than heaert beat )
// eg : heartbeart = 3, keyttl = 9,
// suggestion : keyTTL is three times larger than heartbeart
func (this *EtcdServices) SetKeyTTL(t int64) error {
	if t <= this.heartBeat {
		return ErrKeyTTLTime
	}
	this.keyTTL = t
	return nil
}

// Get all nodes which service name matched
func (this *EtcdServices) Get(service string) (*clientv3.GetResponse, error) {
	return this.client.Get(context.TODO(), "/services/"+service, clientv3.WithPrefix())
}

// Register a node to the service,
// service : service name , eg: rediscluster
// nodename : node name, eg: redis-IP-ProcessID
// node value :
// stop : stop channel
// return *clientv3.PutResponse
func (this *EtcdServices) Register(service string, nodename string, nodevalue string, stop chan struct{}) (registered *clientv3.PutResponse, err error) {
	if len(nodevalue) == 0 {
		return nil, ErrInvalidParams
	}

	key := fmt.Sprintf("/services/%s/%s", service, nodename)
	ticker := time.NewTicker((time.Duration)(this.heartBeat-1) * time.Second)

	resp, err := this.client.Grant(context.TODO(), this.keyTTL)
	if err != nil {
		log.Fatal(err)
	}
	registered, err = this.client.Put(context.TODO(), key, nodevalue, clientv3.WithLease(resp.ID))
	if err != nil {
		return
	}
	go func() {
		for {
			select {
			case <-stop:
				_, err = this.client.Delete(context.TODO(), key)
				ticker.Stop()
				return
			case <-ticker.C:
				resp, err := this.client.Grant(context.TODO(), this.heartBeat)
				if err != nil {
					log.Fatal(err)
				}
				_, err = this.client.Put(context.TODO(), key, nodevalue, clientv3.WithLease(resp.ID))
				for err != nil {
					//errEtcd := err.(*clientv3.EtcdError)
					//logger.Printf("register %v fail : %v [clusters: %v]", service, errEtcd.Message, this.client.GetCluster())
					time.Sleep(2 * time.Second)
					_, err = this.client.Put(context.TODO(), key, nodevalue, clientv3.WithLease(resp.ID))
				}
			}
		}
	}()

	return
}

// Subscribe : watch the status change of all nodes of the service
func (this *EtcdServices) Subscribe(service string) clientv3.WatchChan {
	return this.client.Watch(context.Background(), "/services/"+service, clientv3.WithPrefix())
}

func (this *EtcdServices) subscribetype(service string, tt int) clientv3.WatchChan {
	responseChan := make(chan clientv3.WatchResponse)
	responses := this.Subscribe(service)
	isSpecOp := func(res clientv3.WatchResponse, wt string) bool {
		switch wt {
		case "create":
			for _, e := range res.Events {
				// jb, _ := json.Marshal(e)
				// fmt.Println("debug: ", string(jb), "Modify", e.IsModify(), "Create", e.IsCreate(), "Type", e.Type)
				return e.IsCreate()
			}
		case "update":
			for _, e := range res.Events {
				return e.IsModify()
			}
		case "delete":
			for _, e := range res.Events {
				if e.Type.String() == "DELETE" {
					return true
				}
			}
		}

		return false
	}
	go func() {
		for response := range responses {
			switch tt {
			case New:
				if isSpecOp(response, "create") {
					responseChan <- response
				}
			case Down:
				if isSpecOp(response, "delete") {
					responseChan <- response
				}
			case Update:
				if isSpecOp(response, "update") {
					responseChan <- response
				}
			}
		}
	}()
	return responseChan
}

// SubscribeDown : watch the status is expire or delete of all nodes
func (this *EtcdServices) SubscribeDown(service string) clientv3.WatchChan {
	return this.subscribetype(service, Down)
}

// SubscribeDown : watch the status is create or new of all nodes
func (this *EtcdServices) SubscribeNew(service string) clientv3.WatchChan {
	return this.subscribetype(service, New)
}

// SubscribeDown : watch the status is update of all nodes
func (this *EtcdServices) SubscribeUpdate(service string) clientv3.WatchChan {
	return this.subscribetype(service, Update)
}
