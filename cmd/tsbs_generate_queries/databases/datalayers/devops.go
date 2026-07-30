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
	scenario string
	vcpu     int
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts.
func (d *Devops) GroupByTime(q query.Query, nHosts, numMetrics int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	durationHours := int(duration.Hours())
	var hintKey string
	switch {
	case nHosts <= 1 && durationHours <= 1:
		hintKey = "groupby-1host-1h"
	case nHosts <= 1 && durationHours > 1:
		hintKey = "groupby-1host-12h"
	case numMetrics <= 1:
		hintKey = "groupby-8host-1h-1m"
	default:
		hintKey = "groupby-8host-1h-5m"
	}
	hintStr := d.hint(d.parallelDegreeFor(hintKey))

	sql := fmt.Sprintf(`
			SELECT %s date_trunc('minute', ts) AS minute, 
			%s
			FROM cpu
			WHERE %s 
			AND ts >= '%s' AND ts < '%s' 
			GROUP BY minute 
			ORDER BY minute ASC`,
		hintStr,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.StartString(),
		interval.EndString(),
	)

	humanLabel := fmt.Sprintf("Datalayers %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, duration)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit.
func (d *Devops) GroupByOrderByLimit(q query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	hintStr := d.hint(d.parallelDegreeFor("groupby-orderby-limit"))

	sql := fmt.Sprintf(`SELECT %s date_trunc('minute', ts) AS minute, 
		max(usage_user) 
        FROM cpu 
        WHERE ts < '%s' 
        GROUP BY minute 
        ORDER BY minute DESC 
        LIMIT 5`,
		hintStr,
		interval.EndString(),
	)

	humanLabel := "Datalayers max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day.
func (d *Devops) GroupByTimeAndPrimaryTag(q query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("avg", metrics)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	hintStr := d.hint(d.parallelDegreeFor(doubleGroupByKey(numMetrics)))

	sql := fmt.Sprintf(`SELECT %s date_trunc('hour', ts) AS hour, 
		%s 
		FROM cpu 
		WHERE ts >= '%s' AND ts < '%s' 
		GROUP BY hour, hostname 
		ORDER BY hour`,
		hintStr,
		strings.Join(selectClauses, ", "),
		interval.StartString(),
		interval.EndString(),
	)

	humanLabel := devops.GetDoubleGroupByLabel("Datalayers", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts.
func (d *Devops) MaxAllCPU(q query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	var hintKey string
	if nHosts <= 1 {
		hintKey = "cpu-max-all-1host"
	} else {
		hintKey = "cpu-max-all-8host"
	}
	hintStr := d.hint(d.parallelDegreeFor(hintKey))

	sql := fmt.Sprintf(`SELECT %s date_trunc('hour', ts) AS hour, 
        %s 
        FROM cpu 
        WHERE %s 
		AND ts >= '%s' AND ts < '%s' 
        GROUP BY hour 
		ORDER BY hour`,
		hintStr,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.StartString(),
		interval.EndString(),
	)

	humanLabel := devops.GetMaxAllLabel("Datalayers", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(q query.Query) {
	hintStr := d.hint(d.parallelDegreeFor("lastpoint"))

	sql := fmt.Sprintf(`SELECT %s
		last_value(hostname ORDER BY ts),
		last_value(region ORDER BY ts),
		last_value(datacenter ORDER BY ts),
		last_value(rack ORDER BY ts),
		last_value(os ORDER BY ts),
		last_value(arch ORDER BY ts),
		last_value(team ORDER BY ts),
		last_value(service ORDER BY ts),
		last_value(service_version ORDER BY ts),
		last_value(service_environment ORDER BY ts),
		last_value(usage_user ORDER BY ts),
		last_value(usage_system ORDER BY ts),
		last_value(usage_idle ORDER BY ts),
		last_value(usage_nice ORDER BY ts),
		last_value(usage_iowait ORDER BY ts),
		last_value(usage_irq ORDER BY ts),
		last_value(usage_softirq ORDER BY ts),
		last_value(usage_steal ORDER BY ts),
		last_value(usage_guest ORDER BY ts),
		last_value(usage_guest_nice ORDER BY ts)
		FROM cpu
		GROUP BY hostname`, hintStr)

	humanLabel := "Datalayers last row per host"
	humanDesc := humanLabel
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts).
func (d *Devops) HighCPUForHosts(q query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = "AND " + d.getHostWhereString(nHosts)
	}

	var hintKey string
	if nHosts == 0 {
		hintKey = "high-cpu-all"
	} else {
		hintKey = "high-cpu-1host"
	}
	hintStr := d.hint(d.parallelDegreeFor(hintKey))

	sql := fmt.Sprintf(`SELECT %s * 
		FROM cpu 
		WHERE usage_user > 90.0 
		AND ts >= '%s' AND ts < '%s' 
		%s`,
		hintStr,
		interval.StartString(),
		interval.EndString(),
		hostWhereClause,
	)

	humanLabel, err := devops.GetHighCPULabel("Datalayers", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(q, humanLabel, humanDesc, sql)
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
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
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}
	return selectClauses
}

// doubleGroupByKey returns the hint key for GroupByTimeAndPrimaryTag based on numMetrics.
func doubleGroupByKey(numMetrics int) string {
	switch numMetrics {
	case 1:
		return "double-groupby-1"
	case 5:
		return "double-groupby-5"
	default:
		return "double-groupby-all"
	}
}
