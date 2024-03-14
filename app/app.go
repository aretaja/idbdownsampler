package app

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aretaja/idbdownsampler/config"
	"github.com/aretaja/idbdownsampler/db"
	"github.com/aretaja/idbdownsampler/helpers"
	"github.com/kr/pretty"
)

// main application parameters
type App struct {
	conf          *config.Configuration
	Version       string
	startTS       time.Time
	dsCollections []string
	db            db.Influx
}

// Initialize initializes the App struct by setting up configuration, database connection, memory limits, aggregation counts, and cardinality levels.
//
// This function does not take any parameters and does not return any values.
func (a *App) Initialize() {
	a.startTS = time.Now()

	// Check if config can be obtained
	c, err := config.GetConfig()
	if err != nil {
		log.Fatalf("failed to get config: %v", err)
	}
	a.conf = c

	// Check if config parameters are valid
	if c.DbURL == "" || c.Token == "" || c.Org == "" || c.StatsBucket == "" {
		log.Fatal("invalid config: missing required parameters")
	}

	// Check if collections are provided
	if c.DsCollections == "" {
		log.Fatal("no collections for downsampling provided, interrupting")
	}

	// Create Influx instance
	a.db = db.NewInflux(c.DbURL, c.Token, c.Org, c.StatsBucket, 600)

	// Set memory limit if provided
	if c.MemLimit > 0 {
		a.db.DsMemLimit = c.MemLimit
	}

	// Set aggregation count if provided
	if c.AggrCnt > 0 {
		a.db.AggrCnt = c.AggrCnt
	}

	// Set cardinality levels if provided
	if c.CardMedium > 0 {
		a.db.CardMedium = c.CardMedium
	}
	if c.CardHevy > 0 {
		a.db.CardHevy = c.CardHevy
	}

	// Split collections
	a.dsCollections = strings.Split(c.DsCollections, ",")
}

// collectionBuckets returns the collection of buckets for the given collection name.
// It takes a string parameter 's' representing the collection name and returns a slice of db.Bucket and an error.
func (a *App) collectionBuckets(s string) ([]db.Bucket, error) {
	// iftraffic buckets
	b2d := db.Bucket{
		Name:    "telegraf/2d",
		First:   true,
		AInterv: 2 * time.Minute,
		RPeriod: 48 * time.Hour,
	}

	b7d := db.Bucket{
		Name:    "telegraf/7d",
		From:    &b2d,
		AInterv: 8 * time.Minute,
		RPeriod: 168 * time.Hour,
	}

	b28d := db.Bucket{
		Name:    "telegraf/28d",
		From:    &b7d,
		AInterv: 30 * time.Minute,
		RPeriod: 672 * time.Hour,
	}

	b730d := db.Bucket{
		Name:    "telegraf/all",
		From:    &b28d,
		AInterv: 180 * time.Minute,
		RPeriod: 17520 * time.Hour,
	}

	// icingachk buckets
	b1w := db.Bucket{
		Name:    "icinga2/one_week",
		First:   true,
		AInterv: 1 * time.Minute,
		RPeriod: 168 * time.Hour,
	}

	b4w := db.Bucket{
		Name:    "icinga2/four_weeks",
		From:    &b1w,
		AInterv: 30 * time.Minute,
		RPeriod: 672 * time.Hour,
	}

	ball := db.Bucket{
		Name:    "icinga2/all",
		From:    &b4w,
		AInterv: 180 * time.Minute,
		RPeriod: 17520 * time.Hour,
	}

	collections := make(map[string][]db.Bucket)
	collections["iftraffic"] = []db.Bucket{b2d, b7d, b28d, b730d}
	collections["icingachk"] = []db.Bucket{b1w, b4w, ball}

	if c, ok := collections[s]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unknown collection %s", s)
}

// startResMon starts a resource monitor goroutine that continuously checks for running tasks and used memory.
// Toggles the boolean flag a.db.DbHasResources.
//
// No parameters.
// No return types.
func (a *App) startResMon() {
	interv := 10
	ticker := time.NewTicker(time.Duration(interv) * time.Second)
	go func() {
		for range ticker.C {
			// Check for running tasks
			tasks, err := a.db.GetRunningTasks()
			if err != nil {
				helpers.PrintWarn(fmt.Sprintf("pause working, failed to get running tasks: %+v, retry after %ds", err, interv))
				a.db.DbHasResources = false
				continue
			}

			switch {
			case tasks == nil:
				helpers.PrintWarn(fmt.Sprintf("pause working, no running tasks info, retry after %ds", interv))
				a.db.DbHasResources = false
				continue
			case *tasks > 0:
				helpers.PrintWarn(fmt.Sprintf("pause working, %0.f running tasks, retry after %ds", *tasks, interv))
				a.db.DbHasResources = false
				continue
			default:
				helpers.PrintDbg(fmt.Sprintf("%0.f running tasks", *tasks))
			}

			// Check for used memory
			mem, err := a.db.GetMemUsage()
			if err != nil {
				helpers.PrintWarn(fmt.Sprintf("pause working, failed to get mem usage: %+v, retry after %ds", err, interv))
				a.db.DbHasResources = false
				continue
			}

			switch {
			case mem == nil:
				helpers.PrintWarn(fmt.Sprintf("pause working, no allocated memory info, retry after %ds", interv))
				a.db.DbHasResources = false
				continue
			case *mem > a.db.DsMemLimit:
				helpers.PrintWarn(fmt.Sprintf("pause working, memory usage %0.f%%, retry after %ds", *mem, interv))
				a.db.DbHasResources = false
				continue
			default:
				helpers.PrintDbg(fmt.Sprintf("memory usage %0.f%%", *mem))
			}
			a.db.DbHasResources = true
		}
	}()
}

// workOn performs downsampling on buckets of given collection group.
//
// Parameters:
//
//	c: string representing collection
//	cg: string representing collection group
//	buckets: slice of Bucket structs
//	instances: slice of downsample target instances
//
// Return type: error
func (a *App) workOn(c, cg string, buckets []db.Bucket, instances []string) error {
	ts := time.Now()
	firstRun := true
	for {
		il := len(instances)
		helpers.PrintInfo(fmt.Sprintf("collection %s %s instances: %d %s", c, cg, il, time.Since(ts).String()))

		for i := range buckets {
			helpers.PrintDbg(fmt.Sprintf("collection %s, bucket %s, elapsed %s work on instances:\n%# v", c, buckets[i].Name, time.Since(ts).String(), pretty.Formatter(instances)))
			bucket := buckets[i]
			if bucket.First {
				if firstRun {
					continue
				}
				inst, err := a.db.GetDsInstances(&bucket, c)
				if err != nil {
					return err
				}
				instances = inst[cg]
				continue
			} else {
				count := len(instances)
				for i, inst := range instances {
					helpers.PrintDbg(fmt.Sprintf("collection %s, %s instances:\n%# v, bucket:\n%# v", c, cg, pretty.Formatter(inst), pretty.Formatter(bucket)))
					helpers.PrintInfo(fmt.Sprintf("%d/%d %s %s %s %s %s", i+1, count, inst, c, cg, bucket.Name, time.Since(ts).String()))
					count--

					// Check for resources
					for {
						if !a.db.DbHasResources {
							helpers.PrintDbg("pause working for 30s, no resources available")
							time.Sleep(30 * time.Second)
							continue
						}
						break
					}

					err := a.db.Downsample(&bucket, inst, c)
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
			helpers.PrintInfo(fmt.Sprintf("minimum downsample interval is 3h, collection %s %s sleeping %s", c, cg, sd.String()))
			time.Sleep(sd)
		}
		firstRun = false
		ts = time.Now()
	}
}

// Run starts the application and performs downsampling tasks concurrently.
//
// This function does not take any parameters and does not have a return type.
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
		i, err := a.db.GetDsInstances(&buckets[0], c)
		if err != nil {
			helpers.PrintFatal(fmt.Sprintf("can't get buckets for collection %s, interrupting", c))
		}

		// Work on collection instance groups concurrently
		for cgroup, inst := range i {
			go func(wg *sync.WaitGroup, c, cg string, b []db.Bucket, i []string) {
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
