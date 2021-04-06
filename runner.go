package flow

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

type Runner interface {
	Run(*job) error
	Completed(*job) (bool, error)
	CompletedSuccessfully(*job) (bool, error)
	ResourcesUsed(*job) (resourcesUsed, error)
}

// func NewExecutionContext(j *job)

func createJobFile(jobFile, scriptFile string, j *job) error {
	r, err := j.Cmd.Resources()
	if err != nil {
		return fmt.Errorf("failed to get resources for job: %v: %v", j.UUID, err)
	}
	shell := "/bin/bash"
	content := fmt.Sprintf(`singularity exec %s %s %s %s`, r.SingulartyExtraArgs, r.Container, shell, scriptFile)
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

// DummyRunner does not actually run jobs, it just accepts jobs to run and
// always reports that they completed successfully.
type DummyRunner struct{}

func (r DummyRunner) Run(j *job) error {
	return nil
}

func (r DummyRunner) Completed(j *job) (bool, error) {
	return true, nil
}

func (r DummyRunner) CompletedSuccessfully(j *job) (bool, error) {
	return true, nil
}

func (r DummyRunner) ResourcesUsed(j *job) (resourcesUsed, error) {
	return resourcesUsed{}, nil
}

var _ Runner = DummyRunner{}

type LocalRunner struct {
	cmd *exec.Cmd
	err error
}

func NewLocalRunner() *LocalRunner {
	return &LocalRunner{}
}

func (r *LocalRunner) Run(j *job) error {
	dir, err := ioutil.TempDir("", "flow")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	log.Printf("Job temp directory: %s", dir)
	// defer os.RemoveAll(dir)
	jobFn := filepath.Join(dir, "job.sh")
	scriptFn := filepath.Join(dir, "script.sh")
	if err := createJobFile(jobFn, scriptFn, j); err != nil {
		return err
	}
	if err := createScriptFile(scriptFn, j); err != nil {
		return err
	}
	w, err := os.Create(j.Stdout)
	if err != nil {
		return fmt.Errorf("failed to create stdout file: %s, %s", j.Stdout, err)
	}
	defer w.Close()
	r.cmd = exec.Command("bash", jobFn)
	r.cmd.Dir = dir
	r.cmd.Stdout = w
	r.cmd.Stderr = w
	r.err = r.cmd.Run()
	return nil // this is the job was run without error, not that the job completed successfully.
}

func (r *LocalRunner) Completed(j *job) (bool, error) {
	return true, nil
}

func (r *LocalRunner) CompletedSuccessfully(j *job) (bool, error) {
	return r.err == nil, nil
}

func (r *LocalRunner) ResourcesUsed(j *job) (resourcesUsed, error) {
	return resourcesUsed{}, nil
}

var _ Runner = &LocalRunner{}
