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
	overrides := make(map[string]interface{})
	if startFromScratch {
		overrides["start_from_scratch"] = true
	}
	if jobRunner != "" {
		overrides["job_runner"] = jobRunner
	}
	tasks.InitConfig(configFile, overrides)
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
