package datalayers

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func TestDevopsGetHostWhereWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		want      string
	}{
		{
			desc:      "single host",
			hostnames: []string{"foo1"},
			want:      "hostname IN ('foo1')",
		},
		{
			desc:      "multi host",
			hostnames: []string{"foo1", "foo2"},
			want:      "hostname IN ('foo1', 'foo2')",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := d.getHostWhereWithHostnames(c.hostnames); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGetHostWhereString(t *testing.T) {
	cases := []struct {
		nHosts int
		want   string
	}{
		{
			nHosts: 1,
			want:   "hostname IN ('host_5')",
		},
		{
			nHosts: 2,
			want:   "hostname IN ('host_5', 'host_9')",
		},
		{
			nHosts: 5,
			want:   "hostname IN ('host_5', 'host_9', 'host_3', 'host_1', 'host_7')",
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := d.getHostWhereString(c.nHosts); got != c.want {
			t.Errorf("incorrect output for %d hosts: got %s want %s", c.nHosts, got, c.want)
		}
	}
}

func TestDevopsGetSelectClausesAggMetrics(t *testing.T) {
	cases := []struct {
		desc    string
		agg     string
		metrics []string
		want    string
	}{
		{
			desc:    "single metric - max",
			agg:     "max",
			metrics: []string{"foo"},
			want:    "max(foo)",
		},
		{
			desc:    "multiple metric - max",
			agg:     "max",
			metrics: []string{"foo", "bar"},
			want:    "max(foo), max(bar)",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want:    "avg(foo), avg(bar)",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := strings.Join(d.getSelectClausesAggMetrics(c.agg, c.metrics), ", "); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGroupByTime(t *testing.T) {
	expectedHumanLabel := "Datalayers 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "Datalayers 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedSQLQuery := `SELECT date_trunc('minute', ts) AS minute, 
        max(usage_user)
        FROM cpu
        WHERE hostname IN ('host_0') 
		AND ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-04T00:00:01Z'
        GROUP BY minute 
		ORDER BY minute ASC`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(time.Hour)
	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	metrics := 1
	nHosts := 1
	duration := time.Second

	q := d.GenerateEmptyQuery()
	d.GroupByTime(q, nHosts, metrics, duration)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedSQLQuery)
}

func TestGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "Datalayers max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "Datalayers max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z"
	expectedSQLQuery := `SELECT date_trunc('minute', ts) AS minute, 
		max(usage_user) 
		FROM cpu 
		WHERE ts < '1970-01-01T01:16:22Z' 
		GROUP BY minute 
		ORDER BY minute DESC 
		LIMIT 5`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(2 * time.Hour)
	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.GroupByOrderByLimit(q)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedSQLQuery)
}

func TestGroupByTimeAndPrimaryTag(t *testing.T) {
	expectedHumanLabel := "Datalayers mean of 1 metrics, all hosts, random 12h0m0s by 1h"
	expectedHumanDesc := "Datalayers mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z"
	expectedSQLQuery := `SELECT date_trunc('hour', ts) AS hour, 
		avg(usage_user) 
		FROM cpu 
		WHERE ts >= '1970-01-01T00:16:22Z' AND ts < '1970-01-01T12:16:22Z' 
		GROUP BY hour, hostname 
		ORDER BY hour`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	numMetrics := 1

	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.GroupByTimeAndPrimaryTag(q, numMetrics)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedSQLQuery)
}

func TestMaxAllCPU(t *testing.T) {
	expectedHumanLabel := "Datalayers max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h"
	expectedHumanDesc := "Datalayers max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:16:22Z"
	expectedSQLQuery := `SELECT date_trunc('hour', ts) AS hour, 
        max(usage_user), 
		max(usage_system), 
		max(usage_idle), 
		max(usage_nice), 
		max(usage_iowait), 
		max(usage_irq), 
		max(usage_softirq), 
		max(usage_steal), 
		max(usage_guest), 
		max(usage_guest_nice) 
        FROM cpu 
        WHERE hostname IN ('host_9') 
		AND ts >= '1970-01-01T00:16:22Z' AND ts < '1970-01-01T08:16:22Z' 
        GROUP BY hour 
		ORDER BY hour`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.MaxAllDuration).Add(time.Hour)

	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.MaxAllCPU(q, 1, devops.MaxAllDuration)
	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedSQLQuery)
}

func TestLastPointPerHost(t *testing.T) {
	expectedHumanLabel := "Datalayers last row per host"
	expectedHumanDesc := "Datalayers last row per host"
	expectedSQLQuery := `WITH ranked_cpu AS (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY hostname ORDER BY ts DESC) as row_num 
		FROM cpu
		) 
		SELECT * 
		FROM ranked_cpu 
		WHERE row_num = 1`

	rand.Seed(123) // Setting seed for testing purposes.

	b := BaseGenerator{}
	dq, err := b.NewDevops(time.Now(), time.Now(), 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.LastPointPerHost(q)
	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedSQLQuery)
}

func TestHighCPUForHosts(t *testing.T) {
	cases := []struct {
		desc               string
		nHosts             int
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedHypertable string
		expectedSQLQuery   string
	}{
		{
			desc:               "zero hosts",
			nHosts:             0,
			expectedHumanLabel: "Datalayers CPU over threshold, all hosts",
			expectedHumanDesc:  "Datalayers CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedSQLQuery: `SELECT * 
				FROM cpu 
				WHERE usage_user > 90.0 
				AND ts >= '1970-01-01T00:16:22Z' AND ts < '1970-01-01T12:16:22Z'`,
		},
		{
			desc:               "one host",
			nHosts:             1,
			expectedHumanLabel: "Datalayers CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "Datalayers CPU over threshold, 1 host(s): 1970-01-01T00:54:10Z",
			expectedSQLQuery: `SELECT * 
				FROM cpu 
				WHERE usage_user > 90.0 
				AND ts >= '1970-01-01T00:54:10Z' AND ts < '1970-01-01T12:54:10Z' 
				AND hostname IN ('host_3')`,
		},
		{
			desc:               "five hosts",
			nHosts:             5,
			expectedHumanLabel: "Datalayers CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "Datalayers CPU over threshold, 5 host(s): 1970-01-01T00:37:12Z",
			expectedSQLQuery: `SELECT * 
				FROM cpu 
				WHERE usage_user > 90.0 
				AND ts >= '1970-01-01T00:37:12Z' AND ts < '1970-01-01T12:37:12Z' 
				AND hostname IN ('host_9', 'host_5', 'host_1', 'host_7', 'host_2')`,
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.HighCPUDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewDevops(s, e, 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.HighCPUForHosts(q, c.nHosts)

			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
		})
	}
}

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, sqlQuery string) {
	flightSqlQuery, ok := q.(*query.FlightSqlQuery)

	if !ok {
		t.Fatal("Filled query is not *query.Datalayers type")
	}

	if got := string(flightSqlQuery.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(flightSqlQuery.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	got := tokenize(string(flightSqlQuery.RawQuery))
	expected := tokenize(sqlQuery)
	if len(got) != len(expected) {
		t.Errorf("inconsistent length: got = %v, want = %v", len(got), len(expected))
		t.Errorf("incorrect SQL query:\ngot\n%s\nwant\n%s", string(flightSqlQuery.RawQuery), sqlQuery)
		return
	}

	for i := 0; i < len(got); i++ {
		if got[i] != expected[i] {
			t.Errorf("inconsistent token: got = %s, want = %s", got[i], expected[i])
			t.Errorf("incorrect SQL query:\ngot\n%s\nwant\n%s", string(flightSqlQuery.RawQuery), sqlQuery)
			return
		}
	}
}

func tokenize(raw string) []string {
	tokens := make([]string, 0)
	for _, s := range strings.Split(raw, " ") {
		token := strings.TrimSpace(s)
		if len(token) > 0 {
			tokens = append(tokens, token)
		}
	}
	return tokens
}
