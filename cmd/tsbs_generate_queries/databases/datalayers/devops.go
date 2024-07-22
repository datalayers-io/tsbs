package datalayers

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func panicIfErr(err error) {
	databases.PanicIfErr(err)
}

// Devops produces Datalayers-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE hostname IN ('$HOSTNAME_1',...,'$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(q query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	sql := fmt.Sprintf(`SELECT date_trunc('minute', ts) AS minute, 
        %s
        FROM cpu
        WHERE %s 
		AND ts >= '%v' AND ts < '%v'
        GROUP BY minute 
		ORDER BY minute ASC`,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.StartUnixNano(),
		interval.EndUnixNano(),
	)

	humanLabel := fmt.Sprintf("Datalayers %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(q query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	sql := fmt.Sprintf(`SELECT date_trunc('minute', ts) AS minute, max(usage_user)
        FROM cpu
        WHERE ts < '%v'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5`,
		interval.EndUnixNano(),
	)

	humanLabel := "Datalayers max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(q query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("mean", metrics)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	sql := fmt.Sprintf(`SELECT date_trunc('hour', ts) AS hour, 
		%s,
		FROM cpu 
		WHERE ts >= '%v' AND ts < '%v' 
		GROUP BY hour, hostname
		ORDER BY hour`,
		strings.Join(selectClauses, ", "),
		interval.StartUnixNano(),
		interval.EndUnixNano(),
	)

	humanLabel := devops.GetDoubleGroupByLabel("Datalayers", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE hostname IN ('$HOSTNAME_1',...,'$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(q query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`SELECT date_trunc('hour', ts) AS hour,
        %s
        FROM cpu
        WHERE %s 
		AND ts >= '%v' AND ts < '%v'
        GROUP BY hour 
		ORDER BY hour`,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.StartUnixNano(),
		interval.EndUnixNano(),
	)

	humanLabel := devops.GetMaxAllLabel("Datalayers", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(q query.Query) {
	sql := `SELECT * 
		FROM cpu 
		GROUP BY hostname
		ORDER BY ts DESC
		LIMIT 1`

	humanLabel := "Datalayers last row per host"
	humanDesc := humanLabel
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(q query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = d.getHostWhereString(nHosts)
	}

	sql := fmt.Sprintf(`SELECT *
		FROM cpu
		WHERE usage_user > 90.0
		AND ts >= '%v' AND ts < '%v'
		AND %s`,
		interval.StartUnixNano(),
		interval.EndUnixNano(),
		hostWhereClause,
	)

	humanLabel, err := devops.GetHighCPULabel("Datalayers", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
	// using the OR logic here is an anti-pattern for the query planner. Doing
	// the IN will get translated to an ANY query and do better
	return fmt.Sprintf("hostname IN (%s)", strings.Join(hostnameClauses, ", "))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%v(%v)", agg, m)
	}
	return selectClauses
}
