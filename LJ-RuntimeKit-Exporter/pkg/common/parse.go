/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/sirupsen/logrus"
)

// promMetricType defines the valid Prometheus metric types.
var promMetricType = map[string]bool{
	"gauge":     true,
	"counter":   true,
	"histogram": true,
	"summary":   true,
	"label":     true,
}

// Counter represents a single metric counter definition parsed from CSV.
type Counter struct {
	PodMap     bool
	PullOnly   bool
	FieldID    int
	RepEvent   bool
	FieldName  string
	MetricName string
	ShortName  string
	PromType   string
	Help       string
}

// ExtractAllCountersNotVerify parses the CSV file and returns all counter definitions.
func ExtractAllCountersNotVerify(collectorsFile string, rmDuplicate bool) ([]Counter, error) {
	var err error
	var records [][]string

	records, err = ReadCSVFile(collectorsFile)
	if err != nil {
		logrus.Errorf("Could not read metrics file '%s': %v\n", collectorsFile, err)
		return nil, err
	}

	counters, err := processCounterRecords(records, rmDuplicate)
	if err != nil {
		return nil, err
	}

	return counters, nil
}

// ReadCSVFile reads and parses a CSV file, treating lines starting with '#' as comments.
func ReadCSVFile(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	r := csv.NewReader(file)
	r.Comment = '#'
	records, err := r.ReadAll()

	return records, err
}

// processCounterRecords converts parsed CSV records into Counter structs.
func processCounterRecords(records [][]string, rmDuplicate bool) ([]Counter, error) {
	if len(records) == 0 {
		return nil, nil
	}
	counters := make([]Counter, 0, len(records))
	fieldSet := make(map[int]struct{})

	for i, record := range records {
		if len(record) == 0 {
			continue
		}
		// Trim spaces only if necessary
		for j := range record {
			record[j] = strings.TrimSpace(record[j])
		}

		if len(record) != 8 {
			return nil, fmt.Errorf("malformed CSV record, failed to parse line %d (`%v`), expected 8 fields", i, record)
		}

		var fieldID int
		var err error

		switch {
		case strings.HasPrefix(record[0], DCGM_FIELD_NAME_PREFIX):
			fieldID, err = getDCGMFieldID(record[0])
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(record[0], AMPF_FIELD_NAME_PREFIX):
			fieldID, err = getAMPFieldID(record[0])
			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unknown field prefix in record %d: %s", i, record[0])
		}

		if _, ok := promMetricType[record[6]]; !ok {
			return nil, fmt.Errorf("could not find Prometheus metric type %s", record[5])
		}

		if _, ok := fieldSet[fieldID]; ok && rmDuplicate {
			continue
		}

		counters = append(counters, Counter{
			FieldID:    int(fieldID),
			FieldName:  record[0],
			PodMap:     strings.EqualFold(record[1], "true"),
			PullOnly:   strings.EqualFold(record[2], "true"),
			RepEvent:   strings.EqualFold(record[3], "true"),
			MetricName: record[4],
			ShortName:  record[5],
			PromType:   record[6],
			Help:       record[7],
		})
		fieldSet[fieldID] = struct{}{}
	}

	return counters, nil
}

// getDCGMFieldID retrieves the field ID for a DCGM field, handling both new and old field mappings.
func getDCGMFieldID(fieldName string) (int, error) {
	fieldID, ok := dcgm.DCGM_FI[fieldName]
	if !ok {
		fieldID, ok = dcgm.OLD_DCGM_FI[fieldName]
		if !ok {
			return 0, fmt.Errorf("could not find DCGM field %s", fieldName)
		}
	}
	return int(fieldID), nil
}

// getAMPFieldID retrieves the field ID for an AMPERF field.
func getAMPFieldID(fieldName string) (int, error) {
	fieldID, ok := AMPERF_FI[fieldName]
	if !ok {
		return 0, fmt.Errorf("could not find AMPERF field %s", fieldName)
	}
	return int(fieldID), nil
}
