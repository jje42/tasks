`tasks` is a workflow management system, designed primarily to run jobs on a
High Performance Computer (HPC) running PBS Pro or Slurm. Jobs can optionally
be run inside Singularity containers. This is a very specific set of
conditions, and there are many other workflow management systems already
available. `tasks` is written in Go, and so are the workflows that it manages.
It was originally started as an exercise is learning the Go language.

`tasks` is heavily influenced by Queue, a workflow engine by the Broad
Institute's GSA group as part of the GATK software.

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

	"github.com/jje42/tasks"
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

func (c CreateInput) Resources() tasks.Resources {
	return tasks.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}
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

func (c ToUpper) Resources() tasks.Resources {
	return tasks.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}
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

func (c Merge) Resources() tasks.Resources {
	return tasks.Resources{CPUs: 1, Memory: 1, Time: 1, Container: "docker://debian:10"}
}

func main() {
	queue := tasks.Queue{}

	queue.Add(&CreateInput{Word: "hello", Output: "input1.txt"})
	queue.Add(&CreateInput{Word: "world", Output: "input2.txt"})
	queue.Add(&ToUpper{Input: "input1.txt", Output: "output1.txt"})
	queue.Add(&ToUpper{Input: "input2.txt", Output: "output2.txt"})
	queue.Add(&Merge{Inputs: []string{"output1.txt", "output2.txt"}, Output: "final.txt"})

	if err := queue.Run(); err != nil {
		log.Fatal(err)
	}
}
```
