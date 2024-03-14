// Copyright 2024 by Marko Punnar <marko[AT]aretaja.org>
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

// idbdownsampler is external application for InfluxDB downsampling InfluxDB metrics

package main

import (
	"log"

	"github.com/aretaja/idbdownsampler/app"
	"github.com/aretaja/idbdownsampler/helpers"
)

// Version of release
const version = "0.0.1-devel.19"

func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	helpers.PrintInfo("start influxdb downsampler")
	helpers.PrintDbg("initializing app")
	a := new(app.App)
	a.Version = version
	a.Initialize()

	helpers.PrintDbg("app initialized")

	helpers.PrintDbg("running app")
	a.Run()
}
