// Copyright 2024 by Marko Punnar <marko[AT]aretaja.org>
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

// idbdownsampler is external application for downsampling InfluxDB metrics

package main

import (
	"log"

	"github.com/aretaja/idbdownsampler/app"
	"github.com/aretaja/idbdownsampler/helpers"
)

// Version of release
const version string = "v1.0.2"

// main is the entry point of the program.
//
// No parameters.
// No return values.
func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	helpers.PrintInfo("start influxdb downsampler")
	helpers.PrintDbg("initializing app")

	a := &app.App{
		Version: version,
	}

	a.Initialize()

	helpers.PrintDbg("app initialized")

	helpers.PrintDbg("running app")
	a.Run()
}
