package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	influx "github.com/influxdata/influxdb-client-go/v2"
	influxAPI "github.com/influxdata/influxdb-client-go/v2/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Configuration represents a YAML-formatted config file
type Configuration struct {
	Server   Server
	InfluxDB InfluxDB
}

type Server struct {
	ListenAddr string
}

type InfluxDB struct {
	Address           string
	Username          string
	Password          string
	MeasurementPrefix string
	Database          string
	RetentionPolicy   string
	Token             string
	Organization      string
	Bucket            string
	SkipVerifySsl     bool
	FlushInterval     uint
}

// Load a config file and return the Config struct
func LoadConfiguration(configPath string) (*Configuration, error) {
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()
	viper.SetConfigType("yml")

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading config file %s, %s", configPath, err)
	}

	var configuration Configuration
	err = viper.Unmarshal(&configuration)
	if err != nil {
		return nil, fmt.Errorf("unable to decode config into struct, %s", err)
	}

	return &configuration, nil
}

type InfluxWriteConfigError struct{}

func (r *InfluxWriteConfigError) Error() string {
	return "must configure at least one of bucket or database/retention policy"
}

func InfluxConnect(config *Configuration) (influx.Client, influxAPI.WriteAPI, error) {
	var auth string
	if config.InfluxDB.Token != "" {
		auth = config.InfluxDB.Token
	} else if config.InfluxDB.Username != "" && config.InfluxDB.Password != "" {
		auth = fmt.Sprintf("%s:%s", config.InfluxDB.Username, config.InfluxDB.Password)
	} else {
		auth = ""
	}

	var writeDest string
	if config.InfluxDB.Bucket != "" {
		writeDest = config.InfluxDB.Bucket
	} else if config.InfluxDB.Database != "" && config.InfluxDB.RetentionPolicy != "" {
		writeDest = fmt.Sprintf("%s/%s", config.InfluxDB.Database, config.InfluxDB.RetentionPolicy)
	} else {
		return nil, nil, &InfluxWriteConfigError{}
	}

	if config.InfluxDB.FlushInterval == 0 {
		config.InfluxDB.FlushInterval = 30
	}

	options := influx.DefaultOptions().
		SetFlushInterval(1000 * config.InfluxDB.FlushInterval).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: config.InfluxDB.SkipVerifySsl,
		})
	client := influx.NewClientWithOptions(config.InfluxDB.Address, auth, options)

	writeAPI := client.WriteAPI(config.InfluxDB.Organization, writeDest)

	return client, writeAPI, nil
}

func main() {

	// Load the config file based on path provided via CLI or the default
	configLocation := flag.String("config", "config.yaml", "path to configuration file")
	flag.Parse()
	config, err := LoadConfiguration(*configLocation)
	if err != nil {
		log.WithFields(log.Fields{
			"op":    "main.LoadConfiguration",
			"error": err,
		}).Fatal("failed to load configuration")
	}

	// Initialize the InfluxDB connection
	influxClient, writeAPI, err := InfluxConnect(config)
	if err != nil {
		log.WithFields(log.Fields{
			"op":    "main",
			"error": err,
		}).Fatal("failed to initialize InfluxDB connection")
	}
	defer influxClient.Close()
	defer writeAPI.Flush()

	errorsCh := writeAPI.Errors()

	// Monitor InfluxDB write errors
	go func() {
		for err := range errorsCh {
			log.WithFields(log.Fields{
				"op":    "main",
				"error": err,
			}).Error("encountered error on writing to InfluxDB")
		}
	}()

	// Look for SIGTERM or SIGINT
	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)

	r := mux.NewRouter()
	dataCh := make(chan airGradientData, 1)
	r.HandleFunc("/sensors/{id}/measures", mainHandler(dataCh))

	server := &http.Server{
		Addr:         config.Server.ListenAddr,
		Handler:      r,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	// Submit results to InfluxDB
	go func() {
		for dataPoint := range dataCh {
			influxPoint := influx.NewPoint(
				"air_quality",
				map[string]string{
					"id": dataPoint.Id,
				},
				map[string]interface{}{
					"wifi":         dataPoint.Wifi,
					"co2":          dataPoint.C02,
					"pm1":          dataPoint.PM01,
					"pm25":         dataPoint.PM02,
					"pm10":         dataPoint.PM10,
					"pm003":        dataPoint.PM003,
					"tvoc":         dataPoint.TVOC,
					"nox":          dataPoint.NOX,
					"temp":         dataPoint.Temp,
					"rel_humidity": dataPoint.Hum,
				},
				dataPoint.Ts,
			)
			writeAPI.WritePoint(influxPoint)
		}
	}()

	log.Infof("listening on %s", config.Server.ListenAddr)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.WithError(err).Error("failed to run http server")
		}
	}()

	sig := <-cancelCh
	log.WithFields(log.Fields{
		"op": "main",
	}).Info(fmt.Sprintf("caught signal %v, flushing data to InfluxDB", sig))
	writeAPI.Flush()

}
