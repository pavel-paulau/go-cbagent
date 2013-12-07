package main

import (
	"log"
	"time"

	"github.com/pavel-paulau/gateload/api"
	"github.com/pavel-paulau/gateload/workload"
)

const DocsPerUser = 1000000

func main() {
	var config workload.Config
	workload.ReadConfig(&config)

	c := api.SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)

	user := api.UserAuth{"collector", "password", []string{"stats"}}
	c.AddUser(user.Name, user)
	session := api.Session{Name: user.Name, TTL: 2592000}
	cookie := c.CreateSession(user.Name, session)
	c.AddCookie(&cookie)

	for doc := range workload.DocIterator(0, DocsPerUser, config.DocSize, "stats") {
		log.Println("                   S_M__U__N__")

		t0 := time.Now()
		c.PutSingleDoc(doc.Id, doc)
		t1 := time.Now()
		log.Printf("Push latency (ns): %11d\n", t1.Sub(t0)*time.Nanosecond)

		t0 = time.Now()
		c.GetSingleDoc(doc.Id)
		t1 = time.Now()
		log.Printf("Pull latency (ns): %11d\n", t1.Sub(t0)*time.Nanosecond)

		time.Sleep(time.Duration(10000) * time.Millisecond)
	}
}
