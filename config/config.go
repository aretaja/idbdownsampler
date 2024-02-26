package config

import (
	"os"

	"github.com/tkanos/gonfig"
)

// API configuration sruct
type Configuration struct {
	DbURL         string  `env:"IDBDS_DBURL"`
	Token         string  `env:"IDBDS_TOKEN"`
	Org           string  `env:"IDBDS_ORG"`
	StatsBucket   string  `env:"IDBDS_STATSBUCKET"`
	DsCollections string  `env:"IDBDS_DSCOLLECTIONS"`
	MemLimit      float64 `env:"IDBDS_MEMLIMIT"`
	AggrCnt       int     `env:"IDBDS_AGGRCNT"`
}

// Fills Configuration struct. Prefers environment variables
func GetConfig() (*Configuration, error) {
	conf := new(Configuration)

	f := "/opt/idbdownsampler/etc/idbdownsampler.conf"
	if os.Getenv("IDBDS_CONF") != "" {
		f = os.Getenv("IDBDS_CONF")
	}
	if os.Getenv("IDBDS_TESTDB") != "" { /*  */
		f = "/opt/idbdownsampler/etc/idbdownsampler_testdb.conf"
	}

	err := gonfig.GetConf(f, conf)

	return conf, err
}
