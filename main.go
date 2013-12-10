package main

import (
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/pavel-paulau/gateload/api"
	"github.com/pavel-paulau/gateload/workload"
)

const (
	DocsPerUser = 1000000
	MaxSamplers = 100
)

var activeSamplers int

func measureLatency(c *api.SyncGatewayClient, doc api.Doc) {
	timestamp, latency := measurePushLatency(c, doc)
	fmt.Printf("push %d %.1f\n", timestamp, latency)

	timestamp, latency = measurePullLatency(c, doc)
	fmt.Printf("pull %d %.1f\n", timestamp, latency)

	activeSamplers--
}

func measurePushLatency(c *api.SyncGatewayClient, doc api.Doc) (int64, float64) {
	t0 := time.Now()
	c.PutSingleDoc(doc.Id, doc)
	t1 := time.Now().Round(100 * time.Microsecond)
	return t0.UnixNano(), float64(t1.Sub(t0.Round(100*time.Microsecond))) / math.Pow10(6)
}

func measurePullLatency(c *api.SyncGatewayClient, doc api.Doc) (int64, float64) {
	t0 := time.Now()
	c.GetSingleDoc(doc.Id)
	t1 := time.Now().Round(100 * time.Microsecond)
	return t0.UnixNano(), float64(t1.Sub(t0.Round(100*time.Microsecond))) / math.Pow10(6)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var config workload.Config
	workload.ReadConfig(&config)

	c := api.SyncGatewayClient{}
	c.Init(config.Hostname, config.Database)

	user := api.UserAuth{"collector", "password", []string{"stats"}}
	c.AddUser(user.Name, user)
	session := api.Session{Name: user.Name, TTL: 2592000}
	cookie := c.CreateSession(user.Name, session)
	c.AddCookie(&cookie)

	activeSamplers = 0
	for doc := range workload.DocIterator(0, DocsPerUser, config.DocSize, "stats") {
		if activeSamplers < MaxSamplers {
			activeSamplers++
			go measureLatency(&c, doc)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
}
