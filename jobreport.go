// Copyright 2023 Jonathan Ellis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

type jobReport struct {
	w         io.Writer
	csvWriter *csv.Writer
}

func NewJobreport(w io.Writer) (jobReport, error) {
	writer := csv.NewWriter(w)
	record := []string{
		"job_id",
		"analysis_name",
		"exit_status",
		"exec_host",
		"cpupercent",
		"cput",
		"memory_used",
		"walltime_used",
		"cpus_requested",
		"memory_requested",
		"walltime_requested",
	}
	if err := writer.Write(record); err != nil {
		return jobReport{}, fmt.Errorf("unable to write header: %v", err)
	}
	return jobReport{
		w:         w,
		csvWriter: writer,
	}, nil
}

func (r jobReport) Add(j *job, ru resourcesUsed) error {
	record := []string{
		j.ID,
		j.Cmd.AnalysisName(),
		strconv.Itoa(ru.ExitStatus),
		ru.ExecHost,
		strconv.Itoa(ru.CPUPercent),
		"",
		strconv.Itoa(ru.MemoryUsed),
		strconv.Itoa(ru.TimeUsed),
		strconv.Itoa(ru.CPURequested),
		strconv.Itoa(ru.MemoryRequested),
		strconv.Itoa(ru.TimeRequested),
	}
	return r.csvWriter.Write(record)
}

func (r jobReport) Flush() error {
	r.csvWriter.Flush()
	return r.csvWriter.Error()
}
