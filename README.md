`flow` is a workflow management system, designed primarily to run jobs on a
High Performance Computer (HPC) running PBS Pro. All jobs are run inside
Singularity containers.

This is a very specific set of conditions, and there are many other workflow
management systems already available. `flow` is written in Go, and so are the workflows that it manages. It was originally started as an exercise is learning the Go language:

You may want to look at:

- Cromwell (https://github.com/broadinstitute/cromwell) or
- Nextflow (https://www.nextflow.io/)

which both have more features and support.

## Example Workflow

```go
package main

import (
	"fmt"
	"strings"

	"github.com/jje42/flow"
)

type CreateInput struct {
	Output string `type:"output"`
	Word   string
}

func (c CreateInput) AnalysisName() string {
	return "CreateInput"
}

func (c CreateInput) Command() string {
	return fmt.Sprintf("echo %s >%s", c.Word, c.Output)
}

func (c CreateInput) Resources() (flow.Resources, error) {
	return flow.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}, nil
}

type ToUpper struct {
	Input  string `type:"input"`
	Output string `type:"output"`
}

func (c ToUpper) AnalysisName() string {
	return "ToUpper"
}
func (c ToUpper) Command() string {
	return fmt.Sprintf(`cat %s | tr '[:lower:]' '[:upper:]' >%s`, c.Input, c.Output)
}
func (c ToUpper) Resources() (flow.Resources, error) {
	return flow.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}, nil
}

type Merge struct {
	Inputs []string `type:"input"`
	Output string   `type:"output"`
}

func (c Merge) AnalysisName() string {
	return "merge"
}
func (c Merge) Command() string {
	return fmt.Sprintf(`cat %s >%s`, strings.Join(c.Inputs, " "), c.Output)
}
func (c Merge) Resources() (flow.Resources, error) {
	return flow.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}, nil
}

func Workflow(queue *flow.Queue) {
	queue.Add(&CreateInput{Word: "hello", Output: "input1.txt"})
	queue.Add(&CreateInput{Word: "world", Output: "input2.txt"})
	queue.Add(&ToUpper{Input: "input1.txt", Output: "output1.txt"})
	queue.Add(&ToUpper{Input: "input2.txt", Output: "output2.txt"})
	queue.Add(&Merge{Inputs: []string{"output1.txt", "output2.txt"}, Output: "final.txt"})
}
```