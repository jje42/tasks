package flow

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

type job struct {
	Cmd                   Tasker
	UUID                  uuid.UUID
	ID                    string
	Inputs                []string
	Outputs               []string
	Stdout                string
	DoneFile              string
	runnable              bool
	hasCompleted          bool
	completedSuccessfully bool
	Dependencies          []*job
}

func (j job) Command() string {
	return fmt.Sprintf(`set -o errexit
set -o pipefail
set -o verbose
env
%s`, j.Cmd.Command())
}

type graph struct {
	jobs      []*job
	pending   []*job
	running   []*job
	completed []*job
	failed    []*job
}

func newGraph(cmds []Tasker) graph {
	g := graph{}
	for _, cmd := range cmds {
		job := &job{
			Cmd:      cmd,
			UUID:     uuid.New(),
			Inputs:   cmdInputs(cmd),
			Outputs:  cmdOutputs(cmd),
			runnable: true,
		}
		// What if the job has no outputs? Is this an error, if so we should
		// check for this.
		job.Stdout = fmt.Sprintf("%s.out", job.Outputs[0])
		dir, file := filepath.Split(job.Outputs[0])
		job.DoneFile = filepath.Join(dir, fmt.Sprintf(".%s.done", file))
		g.jobs = append(g.jobs, job)
	}
	for _, j := range g.jobs {
		j.Dependencies = dependenciesFor(j, g.jobs)
		g.pending = append(g.pending, j)
	}
	return g
}

func dependenciesFor(j *job, allJobs []*job) []*job {
	ds := []*job{}
	for _, otherJob := range allJobs {
		if j.UUID != otherJob.UUID {
			if hasIntersection(j.Inputs, otherJob.Outputs) {
				ds = append(ds, otherJob)
			}
		}
	}
	return ds
}

// What happens if a job fails? How do we stop subsequent jobs being run while
// still exiting the loop eventually.
func (g graph) Process() error {
	var runner Runner
	var err error
	runnerStr := ConfigGetString("runner")
	if runnerStr != "" {
		log.Printf("Using runner %s", runnerStr)
	} else {
		log.Printf("Using default runner: local")
	}
	switch runnerStr {
	case "pbs":
		runner, err = NewPBSRunner()
		if err != nil {
			return err
		}
	default:
		runner = NewLocalRunner()
	}
	jobReportfile := "jobreport.csv"
	w, err := os.Create(jobReportfile)
	if err != nil {
		return fmt.Errorf("unable to create job report file: %v", err)
	}
	defer w.Close()
	report, err := NewJobreport(w)
	if err != nil {
		return fmt.Errorf("unable to create job report: %v", err)
	}
	for {
		for _, pending := range g.pending {
			displayJob(pending)
			err := runner.Run(pending)
			if err != nil {

			}
			idx, err := jobIndex(pending, g.pending)
			if err != nil {
				return fmt.Errorf("unable to find job in pending list, %v", pending.UUID)
			}
			g.pending = append(g.pending[:idx], g.pending[idx+1:]...)
			g.running = append(g.running, pending)
		}
		for _, running := range g.running {
			completed, err := runner.Completed(running)
			if err != nil {

			}
			if completed {
				successful, err := runner.CompletedSuccessfully(running)
				if err != nil {

				}
				idx, err := jobIndex(running, g.running)
				if err != nil {

				}
				if successful {
					resources, err := runner.ResourcesUsed(running)
					if err != nil {
						log.Printf("Failed to get resources for job: %v: %v", running.UUID, err)
					} else {
						err = report.Add(running, resources)
						if err != nil {
							log.Printf("Unable to update job report file: %v", err)
						}
					}
					g.completed = append(g.completed, running)
					g.running = append(g.running[:idx], g.running[idx+1:]...)
				} else {
					bold := color.New(color.Bold, color.FgRed).SprintfFunc()
					log.Printf("%s: job failed: %v, %v: stdout written to %s", bold("ERROR"), running.UUID, running.ID, running.Stdout)
					g.failed = append(g.failed, running)
					g.running = append(g.running[:idx], g.running[idx+1:]...)
				}
			}
		}

		runnables := findRunnable(g.jobs)
		if len(runnables) == 0 {
			log.Printf("There are no jobs that are runnable")
			break
		}
		for _, runnable := range runnables {
			displayJob(runnable)
			err = runner.Run(runnable)
			if err != nil {
				return fmt.Errorf("failed to run job: %v", err)
			}
			g.running = append(g.running, runnable)
		}
		i := 0
		for _, running := range g.running {
			// Job can either be still running, completed
			// successfully or completed unsuccessfully.
			completed, err := runner.Completed(running)
			if err != nil {
				return fmt.Errorf("failed to determine if job has completed: %s: %s", running.UUID, err)
			}
			if completed {
				running.hasCompleted = true
				successful, err := runner.CompletedSuccessfully(running)
				if err != nil {
					return fmt.Errorf("failed to determine if job completed successfully: %v: %v", running.UUID, err)
				}
				if successful {
					running.completedSuccessfully = true
					resources, err := runner.ResourcesUsed(running)
					if err != nil {
						log.Printf("failed to get resources used for job: %v: %v", running.UUID, err)
					} else {
						// err = updateResourcesUsedFile(resources)
						err = report.Add(running, resources)
						if err != nil {
							log.Printf("Unable to update resources file: %v", err)
						}
					}
					g.completed = append(g.completed, running)
				} else {
					bold := color.New(color.Bold, color.FgRed).SprintFunc()
					log.Printf("%s: job failed: %v, %v: stdout written to %s", bold("ERROR"), running.UUID, running.ID, running.Stdout)
					running.completedSuccessfully = false
					g.failed = append(g.failed, running)
				}

			} else {
				g.running[i] = running
				i++
			}
		}
		for j := i; j < len(g.running); j++ {
			g.running[j] = nil
		}
		g.running = g.running[:i]
		time.Sleep(3 * time.Second)
	}

	err = report.Flush()
	if err != nil {
		return fmt.Errorf("unable to finalise job report file: %v", err)
	}
	if len(g.failed) > 0 {
		boldRed := color.New(color.Bold, color.FgRed).SprintfFunc()
		log.Printf("Workflow completed with %d %s jobs, see stdout for details", len(g.failed), boldRed("FAILED"))
		for _, job := range g.failed {
			log.Printf("%s: %s, output: %s", boldRed("FAILED"), job.UUID, job.Stdout)
		}
	} else {
		greenBold := color.New(color.Bold, color.FgGreen).SprintfFunc()
		log.Printf("Workflow completed %s", greenBold("SUCCESSFULLY"))
	}
	log.Printf("Completed with %d completed and %d failed (running = %d)", len(g.completed), len(g.failed), len(g.running))
	return nil
}

func findRunnable(jobs []*job) []*job {
	r := []*job{}
	for _, job := range jobs {
		runnable := true
		for _, d := range job.Dependencies {
			if !d.completedSuccessfully {
				runnable = false
			}
		}
		if job.hasCompleted {
			runnable = false
		}
		if runnable {
			r = append(r, job)
		}
	}
	return r
}

// func allDone(jobs []*job) bool {
// 	for _, j := range jobs {
// 		if !j.HasCompleted {
// 			return false
// 		}
// 	}
// 	return true
// }

// Return the value (i.e., the path) of all input fields.
func cmdTag(c Tasker, t string) []string {
	inputs := []string{}
	v := reflect.ValueOf(c).Elem()
	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag.Get("type")
		if tag == t {
			val := v.Field(i)
			switch val.Kind() {
			case reflect.String:
				inputs = append(inputs, val.String())
			case reflect.Slice:
				if val.Type().Elem().Name() == "string" {
					for j := 0; j < val.Len(); j++ {
						inputs = append(inputs, val.Index(j).String())
					}
				}
			}
		}
	}
	return inputs
}

func cmdInputs(c Tasker) []string {
	return cmdTag(c, "input")
}

// Return the value (i.e., the path) of all output fields.
func cmdOutputs(c Tasker) []string {
	return cmdTag(c, "output")
}

func hasIntersection(list1, list2 []string) bool {
	for _, i := range list1 {
		for _, j := range list2 {
			if i == j {
				return true
			}
		}
	}
	return false
}

type resourcesUsed struct {
	CPUPercent      int
	MemoryUsed      int
	TimeUsed        int
	CPURequested    int
	MemoryRequested int
	TimeRequested   int
	ExecHost        string
	ExitStatus      int
}

// func updateResourcesUsedFile(r resourcesUsed) error {
// 	fmt.Printf("%+v\n", r)
// 	return nil
// }

func displayJob(j *job) error {
	r, err := j.Cmd.Resources()
	if err != nil {
		return fmt.Errorf("cannot obtain resources: %v", err)
	}
	c := []string{}
	for _, line := range strings.Split(j.Command(), "\n") {
		c = append(c, fmt.Sprintf("  %s", line))
	}
	indentedCmd := strings.Join(c, "\n")
	bold := color.New(color.Bold).SprintFunc()
	log.Printf(`Running Task:
%s: %s
%s: %s
%s: %s
%s: -l select=1:ncpus=%d:mem=%d -l walltime=%d:00:00
%s: %s
%s: %s
%s: %s
%s: %s
%s:
%s

`,
		bold("UUID"), j.UUID,
		bold("Job ID"), j.ID,
		bold("Analysis Name"), j.Cmd.AnalysisName(),
		bold("Resources"), r.CPUs, r.Memory, r.Time,
		bold("Stdout"), j.Stdout,
		bold("DoneFile"), j.DoneFile,
		bold("Container"), r.Container,
		bold("Extra Args"), r.SingulartyExtraArgs,
		bold("Script"), indentedCmd)
	return nil
}

func jobIndex(j *job, list []*job) (int, error) {
	for i, job := range list {
		if job.UUID == j.UUID {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unable to find job in running list, %v", j.UUID)
}
