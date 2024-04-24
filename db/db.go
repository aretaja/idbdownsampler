package db

import (
	"context"
	"fmt"
	"time"

	"github.com/aretaja/idbdownsampler/helpers"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/kr/pretty"
)

// influxdb parameters
type Influx struct {
	Client         influxdb2.Client
	Org            string
	Statsb         string
	DsMemLimit     float64
	AggrCnt        int
	CardMedium     int
	CardHevy       int
	DbHasResources bool
}

// bucket parameters
type Bucket struct {
	From    *Bucket
	Name    string
	AInterv time.Duration
	RPeriod time.Duration
	First   bool
}

// Make new Influxdb struct
func NewInflux(url, token, org, sb string, timeout uint) Influx {
	// Set HTTP request timeout
	opts := influxdb2.DefaultOptions().SetHTTPRequestTimeout(timeout)
	// Create a new client using an InfluxDB server base URL and an authentication token
	client := influxdb2.NewClientWithOptions(url, token, opts)

	db := Influx{
		Client:         client,
		Org:            org,
		DsMemLimit:     40,   // default 40%
		AggrCnt:        8,    // default 8
		Statsb:         sb,   // stats bucket
		CardMedium:     50,   // medium cardinality level for instance in bucket
		CardHevy:       1000, // hevy cardinality level for instance in bucket
		DbHasResources: true, // default
	}

	return db
}

// GetRunningTasks retrieves the count of running tasks from InfluxDB.
//
// Returns a pointer to float64 and an error.
func (i *Influx) GetRunningTasks() (*float64, error) {
	q := `from(bucket: "` + i.Statsb + `")
  |> range(start: -15s)
  |> filter(fn: (r) => r["_measurement"] == "task_executor_total_runs_active"
      and r._field == "gauge")
  |> last()`

	var count *float64

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), q)
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

// GetMemUsage retrieves the memory usage percentage from Influx database.
//
// No parameters.
// Returns a pointer to float64 and an error.
func (i *Influx) GetMemUsage() (*float64, error) {
	q := `bytes_used = from(bucket: "` + i.Statsb + `")
	|> range(start: -15s)
	|> filter(fn: (r) => r._measurement == "go_memstats_alloc_bytes"
	    and r._field == "gauge")
	|> last()

	total_bytes = from(bucket: "` + i.Statsb + `")
		|> range(start: -15s)
		|> filter(fn: (r) => r._measurement == "go_memstats_sys_bytes"
		    and r._field == "gauge")
		|> last()

	join(tables: {key1: bytes_used, key2: total_bytes}, on: ["_time", "_field"], method: "inner")
		|> map(fn: (r) => ({
		_value: (float(v: r._value_key1) / float(v: r._value_key2)) * 100.0
		}))`

	var used *float64

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
	// Get parser flux query result
	result, err := queryAPI.Query(context.Background(), q)
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

// Cardinality retrieves the cardinality for a given instance in a bucket.
//
// Parameters:
//
//	b *Bucket - the bucket object
//	inst string - the instance name
//
// Returns:
//
//	int - the cardinality count
//	error - an error, if any
func (i *Influx) Cardinality(b *Bucket, inst string) (int, error) {
	var c int
	q := `import "influxdata/influxdb"
		influxdb.cardinality(bucket: "` + b.Name + `",
			start: -28d,
			predicate: (r) => r["agent_name"] == "` + inst + `")`

	helpers.PrintDbg(fmt.Sprintf("cardinality query for %s in %s:\n %s", inst, b.Name, q))

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
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

// GetDsInstances retrieves instances for the given bucket based on collection type, and groups them by cardinality.
//
// Parameters:
//
//	b: *Bucket - the bucket for which to retrieve instances
//	c: string - the collection type
//
// Return:
//
//	map[string][]string - a map of instance groups by cardinality
//	error - an error, if any
func (i *Influx) GetDsInstances(b *Bucket, c string) (map[string][]string, error) {
	st := time.Now().Add(-10 * b.AInterv).Unix() // now - 10 * aggregation duration
	var instances []string
	var q string

	// flux query
	switch {
	case c == "ifstats" || c == "iftraffic" || c == "gengauge" || c == "gencounter":
		q = `import "influxdata/influxdb/schema"
		schema.measurementTagValues(
			bucket: "` + b.Name + `",
			measurement: "` + c + `",
			tag: "agent_name",
			start: ` + fmt.Sprintf("%d", st) + `
		)`
	case c == "icingachk":
		q = `from(bucket: "` + b.Name + `")
		|> range(start: ` + fmt.Sprintf("%d", st) + `)
		|> filter(fn: (r) => (r._measurement == "my-hostalive-icmp"
				or r._measurement == "my-hostalive-tcp"
				or r._measurement == "my-hostalive-http")
		    and r._field == "value")
		|> keyValues(keyColumns: ["hostname"])
		|> keep(columns: ["_value"])
		|> unique()`
	default:
		return nil, fmt.Errorf("unknown collection %s", c)
	}
	helpers.PrintDbg(fmt.Sprintf("instances query for %s:\n %s", b.Name, q))

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
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
		card, err := i.Cardinality(b, v)
		if err != nil {
			helpers.PrintWarn(fmt.Sprintf("%s, %s: error getting cardinality - %v. Using highest rank", v, b.Name, err))
		}
		helpers.PrintDbg(fmt.Sprintf("cardinality of %s in %s: %d", v, b.Name, card))

		switch {
		case card < i.CardMedium:
			cInst["light"] = append(cInst["light"], v)
		case card < i.CardHevy:
			cInst["medium"] = append(cInst["medium"], v)
		default:
			cInst["hevy"] = append(cInst["hevy"], v)
		}
	}

	return cInst, nil
}

// LastTS returns the timestamp of the latest data point for a given instance in a bucket based on collection.
//
// Parameters:
//
//	b *Bucket - the bucket to query
//	inst string - the instance name
//	col string - the collection
//
// Return:
//
//	time.Time - the timestamp of the latest data point
//	error - any error that occurred during the query
func (i *Influx) LastTS(b *Bucket, inst, col string) (time.Time, error) {
	now := time.Now()
	// Return timestamp of now - retention period by default
	lt := now.Add(-1 * b.RPeriod)
	// Set query start time to retention period
	fTS := lt
	// Set query start time to retention period of "from" bucket if exists
	if b.From != nil {
		fTS = now.Add(-1 * b.From.RPeriod)
	}
	var f string
	switch col {
	case "ifstats":
		f = `r._measurement == "ifstats"
		    and r["agent_name"] == "` + inst + `"
			and r._field == "ifAdminStatus"`
	case "iftraffic":
		f = `r._measurement == "iftraffic"
			and r["agent_name"] == "` + inst + `"
			and r._field == "ifOperStatus"`
	case "gengauge":
		f = `r._measurement == "gengauge"
			and r["agent_name"] == "` + inst + `"
			and r._field == "InPower"`
	case "gencounter":
		f = `r._measurement == "gencounter"
			and r["agent_name"] == "` + inst + `"
			and r._field == "feCor"`
	case "icingachk":
		f = `(r._measurement == "my-hostalive-icmp"
				or r._measurement == "my-hostalive-tcp"
				or r._measurement == "my-hostalive-http")
		    and r["hostname"] == "` + inst + `"
			and r._field == "value"`
	default:
		return lt, fmt.Errorf("unknown collection %s", col)
	}

	q := `from(bucket: "` + b.Name + `")
			|> range(start: ` + fmt.Sprintf("%d", fTS.Unix()) + `)
			|> filter(fn: (r) => ` + f + `)
			|> group()
			|> last()
			|> keep(columns: ["_time"])`

	helpers.PrintDbg(fmt.Sprintf("lastTS query for %s:\n %s", b.Name, q))

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
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

// Downsample performs downsampling of measurements of the given instance in the bucket based on collection.
// It returns an error, if any.
func (i *Influx) Downsample(b *Bucket, inst string, col string) error {
	// Default range start timestamp for influx query (now - retention period of source bucket)
	now := time.Now()
	// Set default range start time to first measurement time of source bucket
	fTs := now.Add(-1 * b.From.RPeriod)
	helpers.PrintDbg(fmt.Sprintf("set default range start to:\n %# v", pretty.Formatter(fTs)))

	// Get last measurement time from source bucket
	ft, err := i.LastTS(b.From, inst, col)
	if err != nil {
		return fmt.Errorf("%s, %s: error getting last measurement time: %w; skipping instance", b.From.Name, inst, err)
	}
	helpers.PrintDbg(fmt.Sprintf("%s, %s: last measurement time of source bucket:\n %# v", b.From.Name, inst, pretty.Formatter(ft)))

	// Get last measurement time
	t, err := i.LastTS(b, inst, col)
	if err != nil {
		helpers.PrintWarn(fmt.Sprintf("%s, %s: error getting last measurement time - %v; assuming no data", b.Name, inst, err))
	}
	helpers.PrintDbg(fmt.Sprintf("%s, %s: last measurement time:\n %# v", b.Name, inst, pretty.Formatter(t)))

	// Set range start time to last measurment time of bucket
	fTs = t
	helpers.PrintDbg(fmt.Sprintf("set range start to last measurement time - %# v", pretty.Formatter(fTs)))
	if fTs.Add(b.AInterv).Compare(now) >= 0 {
		helpers.PrintDbg(fmt.Sprintf("%s, %s: nothing to downsample yet. Too little time has elapsed since previous aggregation", b.Name, inst))
		return nil
	}

	// Get instance cardinality in source bucket
	card, err := i.Cardinality(b.From, inst)
	if err != nil {
		helpers.PrintWarn(fmt.Sprintf("error getting cardinality: %v. Using default", err))
	}
	helpers.PrintDbg(fmt.Sprintf("cardinality of %s in %s: %d", inst, b.From.Name, card))

	// Set how many aggregations to do at once
	ac := i.AggrCnt
	switch {
	case card != 0 && card < 100:
		ac *= 20
	case card < 1000:
		ac *= 10
	}
	c := time.Duration(ac) * b.AInterv
	helpers.PrintDbg(fmt.Sprintf("set aggregate range for %s to %s", inst, c.String()))

	// Get query client
	queryAPI := i.Client.QueryAPI(i.Org)
	for fTs.Before(ft.Add(-1 * b.AInterv)) {
		tTs := fTs.Add(c)
		// End time should be before source bucket last time
		for {
			if tTs.Before(ft) {
				break
			}
			tTs = tTs.Add(-1 * b.AInterv)
			helpers.PrintDbg(fmt.Sprintf("aggregation range for %s is behind source last record, reducing it by %s", inst, b.AInterv.String()))
		}
		// Check for resources
		for {
			if !i.DbHasResources {
				helpers.PrintDbg("pause downsampling for 30s, no resources available")
				time.Sleep(30 * time.Second)
				continue
			}
			break
		}

		var q string
		switch {
		case b.From.First && col == "ifstats":
			q = `allData =
			from(bucket: "` + b.From.Name + `")
			  |> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
			  |> filter(fn: (r) => r._measurement == "ifstats"
			      and r["agent_name"] == "` + inst + `")

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
				|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
				|> set(key: "aggregate", value: "last")
				|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

			toCountPsData
				|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
				|> map(fn: (r) => ({r with _field: r._field + "Max"}))
				|> set(key: "aggregate", value: "max")
				|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

			toCountPsData
				|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
				|> map(fn: (r) => ({r with _field: r._field + "Min"}))
				|> set(key: "aggregate", value: "min")
				|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

			toMaxData
				|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
				|> set(key: "aggregate", value: "max")
				|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case !b.From.First && col == "ifstats":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
					|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "ifstats"
					    and r["agent_name"] == "` + inst + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "max")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "min")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "last")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case b.From.First && col == "iftraffic":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
				  |> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
				  |> filter(fn: (r) => r._measurement == "iftraffic"
					  and r["agent_name"] == "` + inst + `")

				toCounterData =
					allData
						|> filter(fn: (r) => r._field == "ifHCInOctets" or r._field == "ifHCOutOctets")

				toCountPsData =
					toCounterData
						|> derivative(unit: 1s, nonNegative: true, columns: ["_value"], timeColumn: "_time")

				toMaxData =
					allData
						|> filter(fn: (r) => r._field == "ifOperStatus")

				toCounterData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toCountPsData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Max"}))
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toCountPsData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Min"}))
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toMaxData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case !b.From.First && col == "iftraffic":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
					|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "iftraffic"
						and r["agent_name"] == "` + inst + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "max")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "min")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "last")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case b.From.First && col == "gengauge":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
				  	|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "gengauge"
						and r["agent_name"] == "` + inst + `")

				allData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: mean, createEmpty: false)
					|> set(key: "aggregate", value: "mean")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Max"}))
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Min"}))
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case !b.From.First && col == "gengauge":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
					|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "gengauge"
						and r["agent_name"] == "` + inst + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "mean")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: mean, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "max")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "min")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case b.From.First && col == "gencounter":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
				  |> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
				  |> filter(fn: (r) => r._measurement == "gencounter"
					  and r["agent_name"] == "` + inst + `")

				toCountPsData =
						allData
						|> derivative(unit: 1s, nonNegative: true, columns: ["_value"], timeColumn: "_time")

				allData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toCountPsData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Max"}))
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toCountPsData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> map(fn: (r) => ({r with _field: r._field + "Min"}))
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case !b.From.First && col == "gencounter":
			q = `allData =
				from(bucket: "` + b.From.Name + `")
					|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
					|> filter(fn: (r) => r._measurement == "gencounter"
						and r["agent_name"] == "` + inst + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "max")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "min")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				allData
					|> filter(fn: (r) => r["aggregate"] == "last")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case b.From.First && col == "icingachk":
			q = `allData =
					from(bucket: "` + b.From.Name + `")
						|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
						|> filter(fn: (r) => r["hostname"] == "` + inst + `"
						    and r._field !~ /^(current_attempt|max_check_attempts|state|state_type|execution_time|latency|reachable|acknowledgement|downtime_depth)$/)

				toMeanData =
					allData
						|> filter(fn: (r) => r._field =~ /^value$/)

				toLastData =
					allData
						|> filter(fn: (r) => r._field =~ /^(crit|min|max|warn|unit)$/)

				toMeanData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: mean, createEmpty: false)
					|> set(key: "aggregate", value: "mean")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toMeanData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toMeanData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toLastData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		case !b.From.First && col == "icingachk":
			q = `allData =
					from(bucket: "` + b.From.Name + `")
						|> range(start: ` + fmt.Sprintf("%d", fTs.Unix()) + `, stop: ` + fmt.Sprintf("%d", tTs.Unix()) + `)
						|> filter(fn: (r) => r["hostname"] == "` + inst + `"
						    and r._field !~ /^(current_attempt|max_check_attempts|state|state_type)$/)

				toMeanData =
					allData
						|> filter(fn: (r) => r._field =~ /^(value|execution_time|latency)$/)

				toLastData =
					allData
						|> filter(fn: (r) => r._field =~ /^(reachable|acknowledgement|crit|downtime_depth|min|max|warn|unit)$/)
						|> filter(fn: (r) => r.aggregate == "last")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "mean")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: mean, createEmpty: false)
					|> set(key: "aggregate", value: "mean")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "min")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: min, createEmpty: false)
					|> set(key: "aggregate", value: "min")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toMeanData
					|> filter(fn: (r) => r.aggregate == "max")
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: max, createEmpty: false)
					|> set(key: "aggregate", value: "max")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")

				toLastData
					|> aggregateWindow(every: ` + b.AInterv.String() + `, fn: last, createEmpty: false)
					|> set(key: "aggregate", value: "last")
					|> to(org: "` + i.Org + `", bucket: "` + b.Name + `")`
		default:
			return fmt.Errorf("no downsaple query found, bucket: %s, collection: %s", b.Name, c)
		}

		fTs = fTs.Add(c)

		helpers.PrintDbg(fmt.Sprintf("downsample query for %s:\n %s", b.Name, q))

		// Execute flux query
		_, err = queryAPI.QueryRaw(context.Background(), q, influxdb2.DefaultDialect())
		if err != nil {
			return fmt.Errorf("influx query error - %w", err)
		}
	}

	return nil
}
