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

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jje42/tasks"
	"github.com/spf13/cobra"
)

var (
	version          = "undefined"
	buildDate        = "undefined"
	startFromScratch bool
	jobRunner        string
	configFile       string
	rootCmd          = &cobra.Command{
		Use:     "tasks [flags] <workflow.go>",
		Short:   fmt.Sprintf("tasks (%s built on %s)", version, buildDate),
		Long:    "",
		Version: version,
		Args:    cobra.ExactArgs(1),
		Run:     myMain,
	}
)

func main() {
	rootCmd.SetVersionTemplate(version + "\n")
	rootCmd.Flags().BoolVarP(&startFromScratch, "start-from-scratch", "s", false, "Start from scratch")
	rootCmd.Flags().StringVarP(&jobRunner, "job-runner", "j", "", "Job runner")
	rootCmd.Flags().StringVarP(&configFile, "config", "c", "", "Config file")
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func myMain(cmd *cobra.Command, args []string) {
	// overrides := make(map[string]interface{})
	o := tasks.Options{}
	if startFromScratch {
		// overrides["start_from_scratch"] = true
		o.StartFromScratch = true
	}
	if jobRunner != "" {
		// overrides["job_runner"] = jobRunner
		o.JobRunner = jobRunner
	}

	// tasks.initConfig(configFile, overrides)
	timestamp := makeTimestamp()

	// Log file ----------
	logFile := fmt.Sprintf("tasks_%s.log", timestamp)
	logw, err := os.Create(logFile)
	if err != nil {
		log.Fatalf("Unable to create log file: %s: %v", logFile, err)
	}
	defer logw.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, logw))

	// Config file ----------
	tasks.SafeWriteConfigAs(fmt.Sprintf("tasks_config_%s.yaml", timestamp))

	if err := tasks.RunWorkflow(args[0]); err != nil {
		log.Fatal(err)
	}
}

func makeTimestamp() string {
	t := time.Now()
	return fmt.Sprintf(
		"%d-%02d-%02d_%02d%02d%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
	)
}
