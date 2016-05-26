package etcdservices

import (
	"testing"
	"github.com/coreos/go-etcd/etcd"
	
)


var hosts =  []string{"http://localhost:4001"}
var client = etcd.NewClient(hosts)
var es = NewEtcdServices(client)
func TestSubscribe(t *testing.T) {

	
	stop := make(chan struct{})
	
	respchan, _ := es.Subscribe("redis")
	
	_, err := es.Register("redis", "redis1", "172.0.0.1", stop)
	if err != nil {
		t.Error(err)
	}
	
	resp := <- respchan
	if resp.Action != "set" {
		t.Log(resp.Action)
		t.Fatal(resp.Action)
	}
}


func TestSubscribeUpdate(t *testing.T) {
	
	stop := make(chan struct{})
	
	respchan, _ := es.Subscribe("redis")
	
	_, err := es.Register("redis", "redis1", "172.0.0.1", stop)
	if err != nil {
		t.Error(err)
	}
	
	resp := <- respchan
	t.Log(resp.Action)
	
	resp = <- respchan
	if resp.Action != "update" {
		t.Fatal(resp.Action)
	}
}

func TestSubscribeDown(t *testing.T) {

	stop := make(chan struct{})
	
	respchan, _ := es.Subscribe("redis")
	
	_, err := es.Register("redis", "redis1", "172.0.0.1", stop)
	if err != nil {
		t.Error(err)
	}
	
	resp := <- respchan
	t.Log(resp.Action)
	
	stop <- struct{}{}
	
	resp = <- respchan
	if resp.Action != "delete" {
		t.Fatal(resp.Action)
	}
}

func TestGet(t *testing.T) {
	
	
	resp, err := es.Get("test")
	if err != nil {
		t.Error(err)
	}
	t.Log(resp)
}
