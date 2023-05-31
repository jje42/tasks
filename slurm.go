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
	"errors"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
)

type SlurmRunner struct {
}

func NewSlurmRunner() (*SlurmRunner, error) {
	return &SlurmRunner{}, nil
}

func (r *SlurmRunner) Run(ctx executionContext) error {
	jobName := ctx.job.Cmd.AnalysisName()
	resources := ctx.job.Cmd.Resources()
	tmpdir, err := filepath.Abs(v.GetString("tmpdir"))
	if err != nil {
		return fmt.Errorf("failed to get abs path of tmpdir: %s", err)
	}
	cmd := exec.Command(
		"sbatch",
		"--job-name", jobName,
		"-o", ctx.job.Stdout,
		"--parsable",
		fmt.Sprintf("--export=TMPDIR=%s", tmpdir),
		fmt.Sprintf("--cpus-per-task=%d", resources.CPUs),
		fmt.Sprintf("--mem=%dG", resources.Memory),
		fmt.Sprintf("--time=%02d:00:00", resources.Time),
		ctx.script,
	)
	ctx.job.BatchCommand = strings.Join(cmd.Args, " ")
	cmd.Dir = ctx.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("unable to start job: %v: %v: %v", ctx.job.UUID, err, string(out))
	}
	ctx.job.ID = strings.TrimSuffix(string(out), "\n")
	log.Printf("Job ID: %s", ctx.job.ID)
	return nil
}

func (r *SlurmRunner) Completed(j *job) (bool, error) {
	state, err := jobState(j)
	return (state == "COMPLETED" || state == "FAILED" || state == "CANCELLED"), err
}

func (r *SlurmRunner) CompletedSuccessfully(j *job) (bool, error) {
	state, err := jobState(j)
	return state == "COMPLETED", err
}

func jobState(j *job) (string, error) {
	jobId := fmt.Sprintf("%s.batch", j.ID)
	cmd := exec.Command("sacct", "-j", jobId, "-o", "state", "-n", "-P")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("unable to determine job state: %s: %s", err, string(out))
	}
	state := strings.TrimSuffix(string(out), "\n")
	return state, nil
}

func (r *SlurmRunner) ResourcesUsed(j *job) (resourcesUsed, error) {
	return resourcesUsed{}, nil
}

func (r *SlurmRunner) Kill(j *job) error {
	if j.ID == "" {
		return errors.New("job has no ID")
	}
	cmd := exec.Command("scancel", j.ID)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("unable to kill job %s: %v", j.ID, err)
	}
	return nil
}
