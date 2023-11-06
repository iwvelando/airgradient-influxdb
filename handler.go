package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// Example point {"wifi":-64, "rco2":419, "pm01":4, "pm02":7, "pm10":7, "pm003_count":834, "tvoc_index":3`3, "nox_index":2, "atmp":32.07, "rhum":56}
type airGradientData struct {
	Id    string `json:"-"`
	Ts    time.Time
	Wifi  int     `json:"wifi"`
	C02   int     `json:"rco2"`
	PM01  int     `json:"pm01"`
	PM02  int     `json:"pm02"`
	PM10  int     `json:"pm10"`
	PM003 int     `json:"pm003_count"`
	TVOC  int     `json:"tvoc_index"`
	NOX   int     `json:"nox_index"`
	Temp  float64 `json:"atmp"`
	Hum   int     `json:"rhum"`
}

func mainHandler(dataChannel chan airGradientData) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read HTTP body
		b, err := io.ReadAll(r.Body)
		if err != nil {
			errMsg := "cannot read body"
			log.WithError(err).Error(errMsg)
			http.Error(w, errMsg, http.StatusBadRequest)
			return
		}

		// Unmarshal into airGradientData struct
		var data airGradientData
		if err := json.Unmarshal(b, &data); err != nil {
			errMsg := "failed to unmarshal"
			log.WithError(err).Error(errMsg)
			http.Error(w, errMsg, http.StatusBadRequest)
			return
		}

		vars := mux.Vars(r)
		// Retrieve ID from variable
		instanceId := "null"
		if id, ok := vars["id"]; ok {
			splitted := strings.Split(id, ":")
			if len(splitted) == 2 {
				instanceId = splitted[1]
			}
		}
		data.Id = instanceId
		data.Ts = time.Now()
		dataChannel <- data
	}
}
