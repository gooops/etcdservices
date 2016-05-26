# Etcd services

a golang package for managing services of etcd
> http://github.com/coreos/etcd

## API
---
### initliaze EtcServices 
```
var hosts =  []string{"http://localhost:4001"}
var client = etcd.NewClient(hosts)
var etcservices = NewEtcdServices(client)
```

### Register a service to etcd
```go
	stop := make(chan struct{})
	respchan, _ := etcservices.Subscribe("rediscluster")
	// best node name like this : redis-IP-ProcessID
	response, err := etcservices.Register("rediscluster", "redis1", "172.0.0.1", stop)

```   

### Get all nodes of service
```go
resp, err := etcservice.Get("rediscluster")
nodes := resp.Node.Nodes
```  

### Subscribe the status change of all nodes of the service
```go
respchan, errchan := etcservice.Subscribe("rediscluster")
	for {
		select {
			case r := <- respchan:
			log.Println(r.Action)
			case e := <- errchan:
			log.Println(e)
		}
	}
```

### SubscribeDown : get the nodes which status change to delete or expire
### SubscribeNew : status : create or new
### SubscribeUpdate : status : update



