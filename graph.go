package flow

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

type job struct {
	Cmd          Commander
	UUID         uuid.UUID
	ID           string
	Inputs       []string
	Outputs      []string
	Stdout       string
	doneFile     string
	Dependencies []*job
	hasCompleted bool
}

// Command takes the original command line and allows adding pre- or post-
// commands.
func (j job) Command() string {
	return fmt.Sprintf(`set -o errexit
set -o pipefail
set -o verbose
env
%s`, j.Cmd.Command())
}

func (j job) isRunnable() bool {
	if j.hasCompleted {
		return false
	}
	for _, d := range j.Dependencies {
		if !d.hasCompleted {
			return false
		}
	}
	return true
}

type graph struct {
	jobs      []*job
	pending   []*job
	running   []*job
	completed []*job
	failed    []*job
}

func newGraph(cmds []Commander) (graph, error) {
	g := graph{}
	for _, cmd := range cmds {
		job := &job{
			Cmd:     cmd,
			UUID:    uuid.New(),
			Inputs:  cmdInputs(cmd),
			Outputs: cmdOutputs(cmd),
		}
		// What if the job has no outputs? Is this an error, if so we should
		// check for this.
		job.Stdout = fmt.Sprintf("%s.out", job.Outputs[0])
		dir, file := filepath.Split(job.Outputs[0])
		job.doneFile = filepath.Join(dir, fmt.Sprintf(".%s.done", file))
		g.jobs = append(g.jobs, job)
	}
	for _, j := range g.jobs {
		j.Dependencies = dependenciesFor(j, g.jobs)
		g.pending = append(g.pending, j)
	}

	startFromScratch := true
	if startFromScratch {
		for _, j := range g.jobs {
			err := os.Remove(j.doneFile)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return g, fmt.Errorf("unable to remove done file: %s: %v", j.doneFile, err)
			}
		}
	}
	// If the done file exists for any pending job, mark it as complete and move
	// it to the completed list.
	pendingList := make([]*job, len(g.pending))
	copy(pendingList, g.pending)
	for _, p := range pendingList {
		ok, err := fileExists(p.doneFile)
		if err != nil {
			return g, fmt.Errorf("unable to determine if file exists: %s: %v", p.doneFile, err)
		}
		if ok {
			p.hasCompleted = true
			idx, err := jobIndex(p, g.pending)
			if err != nil {
				return g, fmt.Errorf("unable to find job index: %s: %v", p.UUID, err)
			}
			g.completed = append(g.completed, p)
			g.pending = append(g.pending[:idx], g.pending[idx+1:]...)
		}
	}
	return g, nil
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
	switch runnerStr := v.GetString("job_runner"); runnerStr {
	case "pbs":
		runner, err = NewPBSRunner()
		if err != nil {
			return err
		}
	case "local":
		runner = NewLocalRunner()
	case "dummy":
		runner = DummyRunner{}
	default:
		log.Fatalf("Unknown runner requested: %s", runnerStr)
	}
	// "yyyy-MM-DD_HHmmss"
	t := time.Now()
	timestamp := fmt.Sprintf(
		"%d-%02d-%02d_%02d%02d%02d.csv",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(),
	)
	jobReportfile := fmt.Sprintf("jobreport_%s.csv", timestamp)
	w, err := os.Create(jobReportfile)
	if err != nil {
		return fmt.Errorf("unable to create job report file: %v", err)
	}
	defer w.Close()
	report, err := NewJobreport(w)
	if err != nil {
		return fmt.Errorf("unable to create job report: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// Set a goroutine to gracefully shutdown running jobs if SIGINT or SIGTERM
	// recieved.
	go func() {
		<-sigs
		for _, job := range g.running {
			err := runner.Kill(job)
			if err != nil {
				log.Println(err)
			}
		}
		os.Exit(1)
	}()

	for {
		if len(g.pending) == 0 && len(g.running) == 0 {
			log.Printf("There are no more jobs to run")
			break
		}
		pendingList := make([]*job, len(g.pending))
		copy(pendingList, g.pending)
		for _, pending := range pendingList {
			if pending.isRunnable() {
				// The job will not have an ID until after runner.Run is called
				// (possibly not even then). If we display the job after running
				// it we will get the PBS job ID when using the PBS runner;
				// however, when using the local runner, there will be no
				// display until after the job has completed. Not sure what's
				// better.
				displayJob(pending)
				ctx, err := newExecutionContext(pending)
				if err != nil {
					return fmt.Errorf("failed to create execution context for %s: %v", pending.UUID, err)
				}
				err = runner.Run(ctx)
				if err != nil {
					return fmt.Errorf("unable to run job: %v", err)
				}
				idx, err := jobIndex(pending, g.pending)
				if err != nil {
					return err
				}
				g.pending = append(g.pending[:idx], g.pending[idx+1:]...)
				g.running = append(g.running, pending)
			} else {
				log.Printf("Job is not runnable: %s", pending.UUID)
			}
		}
		runningList := make([]*job, len(g.running))
		copy(runningList, g.running)
		for _, running := range runningList {
			completed, err := runner.Completed(running)
			if err != nil {
				return fmt.Errorf("unable to determine job state: %s: %s", running.ID, err)
			}
			if completed {
				running.hasCompleted = true
				successful, err := runner.CompletedSuccessfully(running)
				if err != nil {
					return fmt.Errorf("unable to determine job state: %s: %s", running.ID, err)
				}
				idx, err := jobIndex(running, g.running)
				if err != nil {
					return err
				}
				if successful {
					// done files are only created on successful completion of a job.
					_, err := os.Create(running.doneFile)
					if err != nil {
						return fmt.Errorf("unable to create done file for job: %s: %s", running.ID, err)
					}
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
		log.Printf("%d pending, %d running, %d done", len(g.pending), len(g.running), len(g.completed)+len(g.failed))
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

type executionContext struct {
	job    *job
	dir    string
	script string
}

func newExecutionContext(j *job) (executionContext, error) {
	cxt := executionContext{
		job: j,
	}
	var err error
	cxt.dir, err = ioutil.TempDir(v.GetString("flowdir"), "flow")
	if err != nil {
		return executionContext{}, fmt.Errorf("failed to create temp directory: %v", err)
	}
	jobFn := filepath.Join(cxt.dir, "job.sh")
	cxt.script = jobFn
	scriptFn := filepath.Join(cxt.dir, "script.sh")
	if err := createJobFile(jobFn, scriptFn, j); err != nil {
		return executionContext{}, fmt.Errorf("unable to create job file: %v", err)
	}
	if err := createScriptFile(scriptFn, j); err != nil {
		return executionContext{}, fmt.Errorf("unable to create script file: %v", err)
	}
	return cxt, nil
}

func createJobFile(jobFile, scriptFile string, j *job) error {
	r, err := j.Cmd.Resources()
	if err != nil {
		return fmt.Errorf("failed to get resources for job: %v: %v", j.UUID, err)
	}
	shell := "/bin/bash"
	singularityBin := v.GetString("singularity_bin")
	if singularityBin == "" {
		singularityBin = "singularity"
	}
	content := fmt.Sprintf(`%s exec %s %s %s %s`, singularityBin, r.SingularityExtraArgs, r.Container, shell, scriptFile)
	if err := ioutil.WriteFile(jobFile, []byte(content), 0664); err != nil {
		return fmt.Errorf("failed to write job script content: %v", err)
	}
	return nil
}

func createScriptFile(scriptFile string, j *job) error {
	content := j.Command()
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0664); err != nil {
		return fmt.Errorf("failed to write script content: %v", err)
	}
	return nil
}

// Return the value (i.e., the path) of all input fields.
func cmdTag(c Commander, t string) []string {
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

func cmdInputs(c Commander) []string {
	return cmdTag(c, "input")
}

// Return the value (i.e., the path) of all output fields.
func cmdOutputs(c Commander) []string {
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
		bold("DoneFile"), j.doneFile,
		bold("Container"), r.Container,
		bold("Extra Args"), r.SingularityExtraArgs,
		bold("Script"), indentedCmd)
	return nil
}

func jobIndex(j *job, list []*job) (int, error) {
	for i, job := range list {
		if job.UUID == j.UUID {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unable to find job in list, %v", j.UUID)
}

func fileExists(fn string) (bool, error) {
	if _, err := os.Stat(fn); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}
