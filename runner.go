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
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

type Runner interface {
	// Run(*job) error
	Run(executionContext) error
	Completed(*job) (bool, error)
	CompletedSuccessfully(*job) (bool, error)
	ResourcesUsed(*job) (resourcesUsed, error)
	Kill(*job) error
}

// DummyRunner does not actually run jobs, it just accepts jobs to run and
// always reports that they completed successfully.
type DummyRunner struct{}

func (r DummyRunner) Run(cxt executionContext) error {
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

func (r DummyRunner) Kill(j *job) error {
	return nil
}

var _ Runner = DummyRunner{}

type LocalRunner struct {
	cmd *exec.Cmd
	err error
}

func NewLocalRunner() *LocalRunner {
	return &LocalRunner{}
}

func (r *LocalRunner) Run(cxt executionContext) error {
	w, err := os.Create(cxt.job.Stdout)
	if err != nil {
		return fmt.Errorf("failed to create stdout file: %s, %s", cxt.job.Stdout, err)
	}
	defer w.Close()
	r.cmd = exec.Command("bash", cxt.script)
	r.cmd.Dir = cxt.dir
	r.cmd.Stdout = w
	r.cmd.Stderr = w
	r.err = r.cmd.Run()
	cxt.job.ID = cxt.job.UUID.String()
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

func (r *LocalRunner) Kill(j *job) error {
	if r.cmd != nil {
		cmd := exec.Command("kill", "-s", "SIGTERM", strconv.Itoa(r.cmd.Process.Pid))
		err := cmd.Run()
		return fmt.Errorf("unable to kill job (PID %d): %v", r.cmd.Process.Pid, err)
	}
	return nil
}

var _ Runner = &LocalRunner{}
