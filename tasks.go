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
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"
	"strings"
	"text/template"

	"github.com/cstanze/stripmargin"
	"github.com/google/uuid"
	"github.com/guumaster/logsymbols"
	"github.com/spf13/viper"
)

var v *viper.Viper = viper.New()

// Console logger
var cLog = log.New(os.Stderr, "", 0)

type Commander interface {
	AnalysisName() string
	Command() string
	Resources() Resources
}

type Resources struct {
	CPUs                 int
	Memory               int
	Time                 int
	Container            string
	SingularityExtraArgs string
}

// Task provides some default implementations for
// Commanders. It can be embedded in a struct to partially
// implement the Commander interface.
type Task struct {
	Name                 string
	CPUs                 int
	Memory               int
	Time                 int
	Container            string
	SingularityExtraArgs string
}

func (t Task) AnalysisName() string {
	name := t.Name
	if name == "" {
		id, err := uuid.NewUUID()
		if err != nil {
			panic(err)
		}
		name = "command-" + id.String()
	}
	return name
}

func (t Task) Resources() Resources {
	cpus := t.CPUs
	if cpus == 0 {
		cpus = 8
	}
	mem := t.Memory
	if mem == 0 {
		mem = 16
	}
	time := t.Time
	if time == 0 {
		time = 24
	}
	return Resources{
		CPUs:                 cpus,
		Memory:               mem,
		Time:                 time,
		Container:            t.Container,
		SingularityExtraArgs: t.SingularityExtraArgs,
	}
}

func (t *Task) SetResources(res Resources) {
	t.CPUs = res.CPUs
	t.Memory = res.Memory
	t.Time = res.Time
	t.Container = res.Container
	t.SingularityExtraArgs = res.SingularityExtraArgs
}

type Queue struct {
	tasks []Commander
}

func (q *Queue) Add(task ...Commander) {
	for _, t := range task {
		if reflect.ValueOf(t).Kind() != reflect.Ptr {
			panic("values passed to Add must be pointers")
		}
		q.tasks = append(q.tasks, t)
	}
}

func (q *Queue) Tasks() []Commander {
	return q.tasks
}

type Options struct {
	Log              string
	ReportLog        string
	StartFromScratch bool
	Quiet            bool
	Tasksdir         string
	Tmpdir           string
	JobRunner        string
	SingularityBin   string
}

func (q *Queue) Run(opts ...Options) error {
	if len(opts) > 1 {
		return errors.New("you cannot provide more than one Options struct")
	}
	var op Options
	if len(opts) == 1 {
		op = opts[0]
	}
	// Need to set up log file
	// log.SetOutput(ioutil.Discard)

	if op.Log != "" {
		w, err := os.OpenFile(op.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("unable to open log file: %w", err)
		}
		defer w.Close()
		log.SetOutput(w)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	if !v.IsSet("tasksdir") {
		m := make(map[string]interface{})
		m["start_from_scratch"] = op.StartFromScratch
		m["quiet"] = op.Quiet
		// It would be better to have initConfig ignore empty strings
		if op.Tasksdir != "" {
			m["tasksdir"] = op.Tasksdir
		}
		if op.Tmpdir != "" {
			m["tmpdir"] = op.Tmpdir
		}
		if op.JobRunner != "" {
			m["jobrunner"] = op.JobRunner
		}
		if op.SingularityBin != "" {
			m["singularitybin"] = op.SingularityBin
		}

		initConfig("", m)
	}
	if len(q.tasks) > 0 {
		log.Printf("Starting workflow with %d jobs", len(q.tasks))
		if !op.Quiet {
			if op.Log != "" {
				fmt.Printf("%v Writing log to %s\n", logsymbols.Info, op.Log)
			}
			if op.StartFromScratch {
				fmt.Printf("%v start_from_scratch = true\n", logsymbols.Info)
			}
			fmt.Printf("%v Starting workflow with %d jobs\n", logsymbols.Success, len(q.tasks))
		}
	} else {
		log.Printf("No jobs where added to the queue, nothing to do!")
		if !op.Quiet {
			fmt.Printf("%v No jobs where added to the queue, nothing to do!", logsymbols.Error)
		}
		return nil
	}
	for _, task := range q.tasks {
		freezeTask(task)
		//r := task.Resources()
		//if r.Container == "" {
		//        return fmt.Errorf("no container specified for task: %v", task.AnalysisName())
		//}
	}
	g, err := newGraph(q.tasks)
	if err != nil {
		return fmt.Errorf("unable to create graph: %v", err)
	}
	err = g.Process(op)
	if err != nil {
		return fmt.Errorf("failed to run workflow: %v", err)
	}
	return nil
}

func freezeTask(c Commander) {
	v := reflect.ValueOf(c).Elem()
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		ft := t.Field(i)
		tag := ft.Tag.Get("type")
		if tag == "input" || tag == "output" {
			val := v.Field(i)
			if val.CanSet() {
				switch kind := val.Kind(); kind {
				case reflect.String:
					var p string
					if val.String() != "" {
						p, _ = filepath.Abs(val.String())
					}
					val.SetString(p)
				case reflect.Slice:
					if val.Type().Elem().Name() != "string" {
						panic("tag type:input or type:output on something that is not []string")
					}
					for j := 0; j < val.Len(); j++ {
						sliceValue := val.Index(j)
						var p string
						if sliceValue.String() != "" {
							p, _ = filepath.Abs(sliceValue.String())
						}
						sliceValue.SetString(p)
					}
				default:
					panic("tag type:input or tag:output on something that is not a string or slice")
				}
			}
		}
	}
}

func resourcesFor(analysisName string) (Resources, error) {
	// Should we provide default resource allocations or just fail?
	// cpus=1;mem=1;time=1 is rarely going to be useful.
	cpus := v.GetInt(fmt.Sprintf("resources.%s.cpus", analysisName))
	if cpus == 0 {
		return Resources{}, fmt.Errorf("no cpus resource for %s", analysisName)
	}
	memory := v.GetInt(fmt.Sprintf("resources.%s.memory", analysisName))
	if memory == 0 {
		return Resources{}, fmt.Errorf("no memory resource for %s", analysisName)
	}
	time := v.GetInt(fmt.Sprintf("resources.%s.time", analysisName))
	if time == 0 {
		return Resources{}, fmt.Errorf("no time resource for %s", analysisName)
	}
	container := v.GetString(fmt.Sprintf("resources.%s.container", analysisName))
	if container == "" {
		return Resources{}, fmt.Errorf("no container resource for %s", analysisName)
	}
	return Resources{
		CPUs:      cpus,
		Memory:    memory,
		Time:      time,
		Container: container,
	}, nil
}

func initConfig(fn string, overrides map[string]interface{}) error {
	jobRunner := "local"
	if _, err := exec.LookPath("qsub"); err == nil {
		jobRunner = "pbs"
	}
	if _, err := exec.LookPath("sbatch"); err == nil {
		jobRunner = "slurm"
	}
	defaults := map[string]interface{}{
		"tasksdir":           ".tasks",
		"tmpdir":             ".tasks/tmp",
		"start_from_scratch": false,
		"job_runner":         jobRunner,
		"singularity_bin":    "singularity",
	}
	v = viper.New()
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	v.SetConfigName("tasks")
	v.SetConfigType("yaml")
	v.AddConfigPath("$HOME/.config/tasks")
	v.SetEnvPrefix("tasks")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Config found but another error was produced
			return fmt.Errorf("failed to read config file: %v", err)
		}
	}
	if fn != "" {
		localconfig := viper.New()
		localconfig.SetConfigFile(fn)
		localconfig.SetEnvPrefix("tasks")
		localconfig.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		localconfig.AutomaticEnv()
		if err := localconfig.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return fmt.Errorf("failed to read local config file: %v", err)
			}
		}
		for _, key := range localconfig.AllKeys() {
			v.Set(key, localconfig.Get(key))
		}
	}
	for key, value := range overrides {
		v.Set(key, value)
	}
	err := os.MkdirAll(v.GetString("tasksdir"), 0755)
	if err != nil {
		return fmt.Errorf("failed to create tasksdir: %s: %v", v.GetString("tasksdir"), err)
	}
	err = os.MkdirAll(v.GetString("tmpdir"), 0755)
	if err != nil {
		return fmt.Errorf("failed to create tmpdir: %s: %s", v.GetString("tmpdir"), err)
	}
	return nil
}

// should this be in the tasks package to make in easier for users to run workflows?
func RunWorkflow(fn string) error {
	if !v.IsSet("tasksdir") {
		// If tasksdir is not set, config has not been initialised. Should we
		// return an error and force the user to init the config?
		initConfig("", map[string]interface{}{})
	}
	workflowFunc, err := loadPlugin(fn)
	if err != nil {
		return fmt.Errorf("failed to load workflow: %v", err)
	}
	queue := &Queue{}
	workflowFunc(queue)
	if err := queue.Run(); err != nil {
		return err
	}
	return nil
}

func nilWorkflowFunc(q *Queue) {}

func loadPlugin(fn string) (func(*Queue), error) {
	log.Printf("Compiling workflow\n")
	pluginFile, err := compileWorkflow(fn)
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to compile workflow: %v", err)
	}
	p, err := plugin.Open(pluginFile)
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to open plugin: %v", err)
	}
	pWorkflow, err := p.Lookup("Workflow")
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to find Workflow function in plugin: %v", err)
	}
	workflowFunc, ok := pWorkflow.(func(*Queue))
	if !ok {
		return nilWorkflowFunc, fmt.Errorf("workflow func found, but it's type is %T", pWorkflow)
	}
	return workflowFunc, nil
}

func compileWorkflow(fn string) (string, error) {
	dir, err := ioutil.TempDir(v.GetString("tasksdir"), "workflow")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %v", err)
	}
	if err := copyFile(fn, fmt.Sprintf("%s/workflow.go", dir)); err != nil {
		return "", fmt.Errorf("failed to copy workflow to temp directory: %v", err)
	}
	// c := exec.Command("go", "mod", "init", "github.com/jje42/workflow")
	// c.Dir = dir
	// if err := c.Run(); err != nil {
	// 	return "", fmt.Errorf("failed to create go.mod: %v", err)
	// }
	// c = exec.Command("go", "mod", "tidy")
	// c.Dir = dir
	// if err := c.Run(); err != nil {
	// 	return "", fmt.Errorf("failed to run go mod tidy: %v", err)
	// }

	cmdl := exec.Command("go", "build", "-buildmode=plugin", "workflow.go")
	cmdl.Dir = dir
	out, err := cmdl.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to compile workflow: %v\n%v", err, string(out))
	}
	return filepath.Join(dir, "workflow.so"), nil
}

func copyFile(src, dst string) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}

func SafeWriteConfigAs(fn string) error {
	return v.SafeWriteConfigAs(fn)
}

// ReadFOFN reads a file of filenames and returns them as a string slice. It
// does not check that the file exist or the user has permission to read them.
func ReadFOFN(fn string) ([]string, error) {
	r, err := os.Open(fn)
	if err != nil {
		return []string{}, err
	}
	defer r.Close()
	scanner := bufio.NewScanner(r)
	xs := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		xs = append(xs, line)
	}
	if err := scanner.Err(); err != nil {
		return []string{}, err
	}
	return xs, nil

}

// RenderTemplate renders the text/template tpl using the data from object.
// This must succeed and return a string, panics on error.
func RenderTemplate(tpl string, object interface{}) string {
	t := template.Must(template.New("script").Parse(tpl))
	var b strings.Builder
	err := t.Execute(&b, object)
	if err != nil {
		panic(err)
	}
	return stripmargin.StripMargin(b.String())
}

type Tasks struct {
	Commands []Commander
	Outputs  map[string]string
}

func TasksDir() string {
	return v.GetString("tasksdir")
}
