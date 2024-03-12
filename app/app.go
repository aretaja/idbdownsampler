package app

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aretaja/idbdownsampler/config"
	"github.com/aretaja/idbdownsampler/helpers"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/kr/pretty"
)

type App struct {
	conf           *config.Configuration
	Version        string
	startTS        time.Time
	dsCollections  []string
	db             influx
	dbHasResources bool
}

// influxdb parameters
type influx struct {
	client     influxdb2.Client
	org        string
	dsMemLimit float64
	aggrCnt    int
}

// bucket parameters
type bucket struct {
	from    *bucket
	name    string
	aInterv time.Duration
	rPeriod time.Duration
	first   bool
}

func (a *App) Initialize() {
	a.startTS = time.Now()

	// Config
	c, err := config.GetConfig()
	if err != nil {
		log.Fatal(err)
	}
	a.conf = c

	// Increase HTTP request timeout
	opts := influxdb2.DefaultOptions().SetHTTPRequestTimeout(600)
	// Create a new client using an InfluxDB server base URL and an authentication token
	client := influxdb2.NewClientWithOptions(c.DbURL, c.Token, opts)

	a.db = influx{
		client:     client,
		org:        c.Org,
		dsMemLimit: 40, // default 40%
		aggrCnt:    8,  // default 8
	}

	// Use memory limit from config when present
	if c.MemLimit > 0 {
		a.db.dsMemLimit = c.MemLimit
	}
	// Use aggregation count  from config when present
	if c.AggrCnt > 0 {
		a.db.aggrCnt = c.AggrCnt
	}

	if c.DsCollections != "" {
		a.dsCollections = strings.Split(c.DsCollections, ",")
	} else {
		helpers.PrintFatal("no collections for downsampling provided, interrupting")
	}
}

func (a *App) collectionBuckets(s string) ([]bucket, error) {
	// iftraffic buckets
	b2d := bucket{
		name:    "telegraf/2d",
		first:   true,
		aInterv: 2 * time.Minute,
		rPeriod: 48 * time.Hour,
	}

	b7d := bucket{
		name:    "telegraf/7d",
		from:    &b2d,
		aInterv: 8 * time.Minute,
		rPeriod: 168 * time.Hour,
	}

	b28d := bucket{
		name:    "telegraf/28d",
		from:    &b7d,
		aInterv: 30 * time.Minute,
		rPeriod: 672 * time.Hour,
	}

	b730d := bucket{
		name:    "telegraf/all",
		from:    &b28d,
		aInterv: 180 * time.Minute,
		rPeriod: 17520 * time.Hour,
	}

	// icingachk buckets
	b1w := bucket{
		name:    "icinga2/one_week",
		first:   true,
		aInterv: 1 * time.Minute,
		rPeriod: 168 * time.Hour,
	}

	b4w := bucket{
		name:    "icinga2/four_weeks",
		from:    &b1w,
		aInterv: 30 * time.Minute,
		rPeriod: 672 * time.Hour,
	}

	ball := bucket{
		name:    "icinga2/all",
		from:    &b4w,
		aInterv: 180 * time.Minute,
		rPeriod: 17520 * time.Hour,
	}

	collections := make(map[string][]bucket)
	collections["iftraffic"] = []bucket{b2d, b7d, b28d, b730d}
	collections["icingachk"] = []bucket{b1w, b4w, ball}

	if c, ok := collections[s]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unknown collection %s", s)
}

func (a *App) getRunningTasks() (*float64, error) {
	qMemUsage := `from(bucket: "` + a.conf.StatsBucket + `")
  |> range(start: -15s)
  |> filter(fn: (r) => r["_measurement"] == "task_executor_total_runs_active" and r._field == "gauge")
  |> last()`

	var count *float64

	// Get query client
	queryAPI := a.db.client.QueryAPI(a.db.org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), qMemUsage)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			if v, ok := result.Record().Value().(float64); ok {
				count = &v
			}
		}
		if result.Err() != nil {
			return count, result.Err()
		}
	} else {
		return count, err
	}

	return count, nil
}

func (a *App) getMemUsage() (*float64, error) {
	qMemUsage := `bytes_used = from(bucket: "` + a.conf.StatsBucket + `")
	|> range(start: -15s)
	|> filter(fn: (r) => r._measurement == "go_memstats_alloc_bytes" and r._field == "gauge")
	|> last()

	total_bytes = from(bucket: "` + a.conf.StatsBucket + `")
		|> range(start: -15s)
		|> filter(fn: (r) => r._measurement == "go_memstats_sys_bytes" and r._field == "gauge")
		|> last()

	join(tables: {key1: bytes_used, key2: total_bytes}, on: ["_time", "_field"], method: "inner")
		|> map(fn: (r) => ({
		_value: (float(v: r._value_key1) / float(v: r._value_key2)) * 100.0
		}))`

	var used *float64

	// Get query client
	queryAPI := a.db.client.QueryAPI(a.db.org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), qMemUsage)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			if v, ok := result.Record().Value().(float64); ok {
				used = &v
			}
		}
		if result.Err() != nil {
			return used, result.Err()
		}
	} else {
		return used, err
	}

	return used, nil
}

func (a *App) startResMon() {
	interv := 10
	ticker := time.NewTicker(time.Duration(interv) * time.Second)
	go func() {
		for range ticker.C {
			// Check for running tasks
			tasks, err := a.getRunningTasks()
			if err != nil {
				helpers.PrintWarn(fmt.Sprintf("pause working, failed to get running tasks: %+v, retry after %ds", err, interv))
				a.dbHasResources = false
				continue
			}

			switch {
			case tasks == nil:
				helpers.PrintWarn(fmt.Sprintf("pause working, no running tasks info, retry after %ds", interv))
				a.dbHasResources = false
				continue
			case *tasks > 0:
				helpers.PrintWarn(fmt.Sprintf("pause working, %0.f running tasks, retry after %ds", *tasks, interv))
				a.dbHasResources = false
				continue
			default:
				helpers.PrintDbg(fmt.Sprintf("%0.f running tasks", *tasks))
			}

			// Check for used memory
			mem, err := a.getMemUsage()
			if err != nil {
				helpers.PrintWarn(fmt.Sprintf("pause working, failed to get mem usage: %+v, retry after %ds", err, interv))
				a.dbHasResources = false
				continue
			}

			switch {
			case mem == nil:
				helpers.PrintWarn(fmt.Sprintf("pause working, no allocated memory info, retry after %ds", interv))
				a.dbHasResources = false
				continue
			case *mem > a.db.dsMemLimit:
				helpers.PrintWarn(fmt.Sprintf("pause working, memory usage %0.f%%, retry after %ds", *mem, interv))
				a.dbHasResources = false
				continue
			default:
				helpers.PrintDbg(fmt.Sprintf("memory usage %0.f%%", *mem))
			}
			a.dbHasResources = true
		}
	}()
}

func (a *App) getDsInstances(b bucket, c string) (map[string][]string, error) {
	st := time.Now().Add(-2 * b.aInterv).Unix() // now - 2 * aggregation duration
	var instances []string
	var q string

	// flux query
	switch c {
	case "iftraffic":
		q = `from(bucket: "` + b.name + `")
		|> range(start: ` + fmt.Sprintf("%d", st) + `)
		|> filter(fn: (r) => r._measurement == "ifstats" and r._field == "ifAdminStatus")
		|> keyValues(keyColumns: ["agent_name"])
		|> keep(columns: ["_value"])
		|> unique()`
	case "icingachk":
		q = `from(bucket: "` + b.name + `")
		|> range(start: ` + fmt.Sprintf("%d", st) + `)
		|> filter(fn: (r) => r._measurement =~ /^my-hostalive-/ and r._field == "value")
		|> keyValues(keyColumns: ["hostname"])
		|> keep(columns: ["_value"])
		|> unique()`
	default:
		return nil, fmt.Errorf("unknown collection %s", c)
	}

	helpers.PrintDbg(fmt.Sprintf("instances query for %s:\n %s", b.name, q))

	// Get query client
	queryAPI := a.db.client.QueryAPI(a.db.org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), q)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			if v, ok := result.Record().Value().(string); ok {
				instances = append(instances, v)
			}
		}
		if result.Err() != nil {
			return nil, result.Err()
		}
	} else {
		return nil, err
	}

	// Group by cardinality
	cInst := make(map[string][]string)
	for _, v := range instances {
		// Get instance cardinality
		card, err := b.cardinality(a.db, v)
		if err != nil {
			helpers.PrintWarn(fmt.Sprintf("%s, %s: error getting cardinality - %v. Using highest rank", v, b.name, err))
		}
		helpers.PrintDbg(fmt.Sprintf("cardinality of %s in %s: %d", v, b.name, card))

		switch {
		case card < a.conf.CardMedium:
			cInst["light"] = append(cInst["light"], v)
		case card < a.conf.CardHevy:
			cInst["medium"] = append(cInst["medium"], v)
		default:
			cInst["hevy"] = append(cInst["hevy"], v)
		}
	}

	return cInst, nil
}

func (b *bucket) lastTS(i influx, inst, col string) (time.Time, error) {
	var lt time.Time
	var f string
	switch col {
	case "iftraffic":
		f = `r._measurement == "ifstats" and r["agent_name"] == "` + inst + `" and r._field == "ifAdminStatus"`
	case "icingachk":
		f = `r._measurement =~ /^my-hostalive-/ and r["hostname"] == "` + inst + `" and r._field == "value"`
	default:
		return lt, fmt.Errorf("unknown collection %s", col)
	}

	q := `allData =
    	from(bucket: "` + b.name + `")
			|> range(start: 0)
			|> filter(fn: (r) => ` + f + `)
			|> keep(columns: ["_time"])
			|> max(column: "_time")
			|> yield()`

	helpers.PrintDbg(fmt.Sprintf("first-last query for %s:\n %s", b.name, q))

	// Get query client
	queryAPI := i.client.QueryAPI(i.org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), q)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			lt = result.Record().Time()
		}
		if result.Err() != nil {
			return lt, result.Err()
		}
	} else {
		return lt, err
	}

	return lt, nil
}

func (b *bucket) cardinality(i influx, inst string) (int, error) {
	var c int
	q := `import "influxdata/influxdb"
		influxdb.cardinality(bucket: "` + b.name + `",
			start: -28d,
			predicate: (r) => r["agent_name"] == "` + inst + `")`

	helpers.PrintDbg(fmt.Sprintf("cardinality query for %s in %s:\n %s", inst, b.name, q))

	// Get query client
	queryAPI := i.client.QueryAPI(i.org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), q)
	if err == nil {
		// Use Next() to iterate over query result lines
		for result.Next() {
			if v, ok := result.Record().Value().(int64); ok {
				c = int(v)
			}
		}
		if result.Err() != nil {
			return c, result.Err()
		}
	} else {
		return c, err
	}

	return c, nil
}

func (a *App) downsample(b bucket, inst string, col string) error {
	// Default range start timestamp for influx query (now - retention period of source bucket)
	now := time.Now()
	// Set default range start time to first measurement time of source bucket
	fTs := now.Add(-1 * b.from.rPeriod)
	helpers.PrintDbg(fmt.Sprintf("set default range start to:\n %# v", pretty.Formatter(fTs)))

	// Get last measurement time from source bucket
	ft, err := b.from.lastTS(a.db, inst, col)
	if err != nil {
		return fmt.Errorf("%s, %s: error getting last measurement time: %w; skipping instance", b.from.name, inst, err)
	}
	helpers.PrintDbg(fmt.Sprintf("%s, %s: last measurement time of source bucket:\n %# v", b.from.name, inst, pretty.Formatter(ft)))

	// Get last measurement time
	t, err := b.lastTS(a.db, inst, col)
	if err != nil {
		helpers.PrintWarn(fmt.Sprintf("%s, %s: error getting last measurement time - %v; assuming no data", b.name, inst, err))
	}
	helpers.PrintDbg(fmt.Sprintf("%s, %s: last measurement time:\n %# v", b.name, inst, pretty.Formatter(t)))

	// Set range start time to last measurment time of bucket
	fTs = t
	helpers.PrintDbg(fmt.Sprintf("set range start to last measurement time - %# v", pretty.Formatter(fTs)))
	if fTs.Add(b.aInterv).Compare(now) >= 0 {
		helpers.PrintDbg(fmt.Sprintf("%s, %s: nothing to downsample yet. Too little time has elapsed since previous aggregation", b.name, inst))
		return nil
	}

	// Get instance cardinality in source bucket
	card, err := b.from.cardinality(a.db, inst)
	if err != nil {
		helpers.PrintWarn(fmt.Sprintf("error getting cardinality: %v. Using default", err))
	}
	helpers.PrintDbg(fmt.Sprintf("cardinality of %s in %s: %d", inst, b.from.name, card))

	// Set how many aggregations to do at once
	ac := a.db.aggrCnt
	switch {
	case card != 0 && card < 100:
		ac *= 20
	case card < 1000:
		ac *= 10
	}
	c := time.Duration(ac) * b.aInterv
	helpers.PrintDbg(fmt.Sprintf("set aggregate range for %s to %s", inst, c.String()))

	// Get query client
	queryAPI := a.db.client.QueryAPI(a.db.org)
	for fTs.Before(ft.Add(-1 * b.aInterv)) {
		tTs := fTs.Add(c)
		// End time should be before source bucket last time
		for {
			if tTs.Before(ft) {
				break
			}
			tTs = tTs.Add(-1 * b.aInterv)
			helpers.PrintDbg(fmt.Sprintf("aggregation range for %s is behind source last record, reducing it by %s", inst, b.aInterv.String()))
		}
		// Check for resources
		for {
			if !a.dbHasResources {
				helpers.PrintDbg("pause downsampling for 30s, no resources available")
				time.Sleep(30 * time.Second)
				continue
			}
			break
		}

		var q string
		switch {
		case b.from.first && col == "iftraffic":
			q = `allData =
			from(bucket: "` + b.from.name + `")
			  |> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
			  |> filter(fn: (r) => r._measurement == "ifstats" and r["agent_name"] == "` + inst + `")

			toCounterData =
				allData
					|> filter(fn: (r) => r._field =~ /^if(?:HC)*(?:In|Out)/)

			toCountPsData =
				toCounterData
					|> derivative(unit: 1s, nonNegative: true, columns: ["_value"], timeColumn: "_time")

			toMaxData =
				allData
					|> filter(fn: (r) => r._field =~ /^(?:ifAdminStatus|ifOperStatus)$/)

			toCounterData
				|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: last, createEmpty: false)
				|> set(key: "aggregate", value: "last")
				|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

			toCountPsData
				|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: max, createEmpty: false)
				|> map(fn: (r) => ({r with _field: r._field + "Max"}))
				|> set(key: "aggregate", value: "max")
				|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

			toCountPsData
				|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: min, createEmpty: false)
				|> map(fn: (r) => ({r with _field: r._field + "Min"}))
				|> set(key: "aggregate", value: "min")
				|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

			toMaxData
				|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: max, createEmpty: false)
				|> set(key: "aggregate", value: "max")
				|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")`
		case !b.from.first && col == "iftraffic":
			q = `allData =
				from(bucket: "` + b.from.name + `")
					|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "ifstats" and r["agent_name"] == "` + inst + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "max")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: max, createEmpty: false)
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "min")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: min, createEmpty: false)
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "last")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: last, createEmpty: false)
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")`
		case b.from.first && col == "icingachk":
			q = `allData =
					from(bucket: "` + b.from.name + `")
						|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
						|> filter(fn: (r) => r["hostname"] == "` + inst + `" and r._field !~ /^(current_attempt|max_check_attempts|state|state_type|execution_time|latency|reachable|acknowledgement|downtime_depth)$/)

				toMeanData =
					allData
						|> filter(fn: (r) => r._field =~ /^value$/)

				toLastData =
					allData
						|> filter(fn: (r) => r._field =~ /^(crit|min|max|warn|unit)$/)

				toMeanData
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: mean, createEmpty: false)
					|> set(key: "aggregate", value: "mean")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toMeanData
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: min, createEmpty: false)
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toMeanData
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: max, createEmpty: false)
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toLastData
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")`
		case !b.from.first && col == "icingachk":
			q = `allData =
					from(bucket: "` + b.from.name + `")
						|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
						|> filter(fn: (r) => r["hostname"] == "` + inst + `" and r._field !~ /^(current_attempt|max_check_attempts|state|state_type)$/)

				toMeanData =
					allData
						|> filter(fn: (r) => r._field =~ /^(value|execution_time|latency)$/)

				toLastData =
					allData
						|> filter(fn: (r) => r._field =~ /^(reachable|acknowledgement|crit|downtime_depth|min|max|warn|unit)$/)
						|> filter(fn: (r) => r.aggregate == "last")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "mean")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: mean, createEmpty: false)
					|> set(key: "aggregate", value: "mean")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "min")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: min, createEmpty: false)
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "max")
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: max, createEmpty: false)
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")

				toLastData
					|> aggregateWindow(every: ` + b.aInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + a.db.org + `", bucket: "` + b.name + `")`
		default:
			return fmt.Errorf("no downsaple query found, bucket: %s, collection: %s", b.name, c)
		}

		fTs = fTs.Add(c)

		helpers.PrintDbg(fmt.Sprintf("downsample query for %s:\n %s", b.name, q))

		// Execute flux query
		_, err = queryAPI.QueryRaw(context.Background(), q, influxdb2.DefaultDialect())
		if err != nil {
			return fmt.Errorf("influx query error - %w", err)
		}
	}

	return nil
}

func (a *App) workOn(c, cg string, buckets []bucket, instances []string) error {
	ts := time.Now()
	firstRun := true
	for {
		il := len(instances)
		helpers.PrintInfo(fmt.Sprintf("collection %s %s instances: %d %s", c, cg, il, time.Since(ts).String()))

		for i := range buckets {
			helpers.PrintDbg(fmt.Sprintf("collection %s, bucket %s, elapsed %s work on instances:\n%# v", c, buckets[i].name, time.Since(ts).String(), pretty.Formatter(instances)))
			bucket := buckets[i]
			if bucket.first {
				if firstRun {
					continue
				}
				inst, err := a.getDsInstances(bucket, c)
				if err != nil {
					return err
				}
				instances = inst[cg]
				continue
			} else {
				count := len(instances)
				for i, inst := range instances {
					helpers.PrintDbg(fmt.Sprintf("collection %s, %s instances:\n%# v, bucket:\n%# v", c, cg, pretty.Formatter(inst), pretty.Formatter(bucket)))
					helpers.PrintInfo(fmt.Sprintf("%d/%d %s %s %s %s %s", i+1, count, inst, c, cg, bucket.name, time.Since(ts).String()))
					count--

					// Check for resources
					for {
						if !a.dbHasResources {
							helpers.PrintDbg("pause working for 30s, no resources available")
							time.Sleep(30 * time.Second)
							continue
						}
						break
					}

					err := a.downsample(bucket, inst, c)
					if err != nil {
						helpers.PrintErr(fmt.Sprintf("error on downsample: %v", err))
						time.Sleep(10 * time.Second)
						continue
					}
				}
			}
		}

		elapsed := time.Since(ts)
		helpers.PrintInfo(fmt.Sprintf("collection %s %s done, elapsed: %s", c, cg, elapsed.String()))
		if elapsed < 3*time.Hour {
			sd := 3*time.Hour - elapsed
			helpers.PrintInfo(fmt.Sprintf("minimum downsample interval is 3h, sleeping %s", sd.String()))
			time.Sleep(sd)
			continue
		}
		firstRun = false
		ts = time.Now()
	}
}

func (a *App) Run() {
	a.startResMon()

	var wg sync.WaitGroup
	wg.Add(1) // add here because we want to stop when even one collection fails
	for _, c := range a.dsCollections {
		// Get buckets
		buckets, err := a.collectionBuckets(c)
		if err != nil {
			helpers.PrintFatal(fmt.Sprintf("can't get buckets for collection %s, interrupting", c))
		}

		// Get instances
		i, err := a.getDsInstances(buckets[0], c)
		if err != nil {
			helpers.PrintFatal(fmt.Sprintf("can't get buckets for collection %s, interrupting", c))
		}

		// Work on collection instance groups concurrently
		for cgroup, inst := range i {
			go func(wg *sync.WaitGroup, c, cg string, b []bucket, i []string) {
				defer wg.Done()
				err := a.workOn(c, cg, b, i)
				if err != nil {
					helpers.PrintErr(fmt.Sprintf("downsample collection %s, %s: %+v", c, cg, err))
				}

				// Set interrupt flag when too little time has elapsed from start
				if time.Since(a.startTS) < 10*time.Second {
					helpers.PrintFatal(fmt.Sprintf("downsampling of %s, %s ended too fast, interrupting", c, cg))
				}
			}(&wg, c, cgroup, buckets, inst)
		}
	}
	wg.Wait()
	helpers.PrintFatal("fatal error, interrupting")
}
