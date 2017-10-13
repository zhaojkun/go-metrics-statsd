package statsd

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

func floatEquals(a, b float64) bool {
	return (a-b) < 0.000001 && (b-a) < 0.000001
}

func ExampleStatsD() {
	go StatsD(metrics.DefaultRegistry, 1*time.Second, "some.prefix", "localhost:7524")
}

func ExampleStatsDWithConfig() {
	go WithConfig(Config{
		Addr:          "localhost:7524",
		Registry:      metrics.DefaultRegistry,
		FlushInterval: 1 * time.Second,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
	})
}

func NewTestServer(t *testing.T, prefix string) (map[string]float64, net.PacketConn, metrics.Registry, Config, chan bool) {
	addr := "127.0.0.1:7524"
	res := make(map[string]float64)

	ln, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}
	got := make(chan bool, 1)
	go func() {
		buf := make([]byte, 1500)
		for {
			n, _, err := ln.ReadFrom(buf[:])
			if err != nil {
				got <- true
				t.Log(err)
				return
			}
			r := bufio.NewReader(bytes.NewReader(buf[:n]))
			line, err := r.ReadString('\n')
			for err == nil {
				parts := strings.Split(line, ":")
				i, _ := strconv.ParseFloat(strings.Split(parts[1], "|")[0], 0)
				if testing.Verbose() {
					t.Log("recv", parts[0], i)
				}
				res[parts[0]] = res[parts[0]] + i
				line, err = r.ReadString('\n')
			}

		}
	}()

	r := metrics.NewRegistry()

	c := Config{
		Addr:          addr,
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        prefix,
	}

	return res, ln, r, c, got
}

func TestWrites(t *testing.T) {
	res, l, r, c, got := NewTestServer(t, "foobar")

	metrics.GetOrRegisterCounter("foo", r).Inc(2)

	// TODO: Use a mock meter rather than wasting 10s to get a QPS.
	for i := 0; i < 10*4; i++ {
		metrics.GetOrRegisterMeter("bar", r).Mark(1)
	}
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 5)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 4)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 3)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 2)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 1)

	statsd(&c)
	time.Sleep(time.Second)
	l.Close()
	<-got
	if expected, found := 2.0, res["foobar.foo.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 40.0, res["foobar.bar.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5.0, res["foobar.baz.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5000.0, res["foobar.baz.99-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 3000.0, res["foobar.baz.50-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}
}
