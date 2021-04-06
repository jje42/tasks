package flow

import (
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/viper"
)

type Commander interface {
	AnalysisName() string
	Command() string
	Resources() (Resources, error)
}

type Resources struct {
	CPUs                int
	Memory              int
	Time                int
	Container           string
	SingulartyExtraArgs string
}

type QueueOptions struct {
	StartFromScratch bool
	JobRunner        string
}

func NewQueueOptions() QueueOptions {
	return QueueOptions{
		StartFromScratch: viper.GetBool("start_from_scratch"),
		JobRunner:        viper.GetString("job_runner"),
	}
}

type Queue struct {
	tasks []Commander
}

func (q *Queue) Add(task Commander) {
	q.tasks = append(q.tasks, task)
}

func (q *Queue) Run() error {
	if len(q.tasks) > 0 {
		log.Printf("Starting workflow with %d jobs", len(q.tasks))
	} else {
		log.Printf("No jobs where added to the queue, nothing to do!")
	}
	for _, task := range q.tasks {
		freezeTask(task)
		r, err := task.Resources()
		if err != nil {
			return fmt.Errorf("failed to get resources for job: %s", task.AnalysisName())
		}
		if r.Container == "" {
			return fmt.Errorf("no container specified for task: %v", task.AnalysisName())
		}
	}
	g, err := newGraph(q.tasks)
	if err != nil {
		return fmt.Errorf("unable to create graph: %v", err)
	}
	err = g.Process()
	if err != nil {
		log.Fatalf("Failed to run workflow: %v", err)
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
					p, _ := filepath.Abs(val.String())
					val.SetString(p)
				case reflect.Slice:
					if val.Type().Elem().Name() != "string" {
						panic("tag type:input or type:output on something that is not []string")
					}
					for j := 0; j < val.Len(); j++ {
						sliceValue := val.Index(j)
						p, _ := filepath.Abs(sliceValue.String())
						sliceValue.SetString(p)
					}
				default:
					panic("tag type:input or tag:output on something that is not a string or slice")
				}
			}
		}
	}
}

func ResourcesFor(analysisName string) (Resources, error) {
	// Should we provide default resource allocations or just fail?
	// cpus=1;mem=1;time=1 is rarely going to be useful.
	cpus := viper.GetInt(fmt.Sprintf("resources.%s.cpus", analysisName))
	if cpus == 0 {
		return Resources{}, fmt.Errorf("no cpus resource for %s", analysisName)
	}
	memory := viper.GetInt(fmt.Sprintf("resources.%s.memory", analysisName))
	if memory == 0 {
		return Resources{}, fmt.Errorf("no memory resource for %s", analysisName)
	}
	time := viper.GetInt(fmt.Sprintf("resources.%s.time", analysisName))
	if time == 0 {
		return Resources{}, fmt.Errorf("no time resource for %s", analysisName)
	}
	container := viper.GetString(fmt.Sprintf("resources.%s.container", analysisName))
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

func init() {
	bold := color.New(color.Bold).SprintfFunc()
	defaults := map[string]interface{}{
		"start_from_scratch": false,
		"job_runner":         "local",
	}
	for key, value := range defaults {
		viper.SetDefault(key, value)
	}
	viper.SetConfigName("flow")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.config/flow")
	viper.SetEnvPrefix("flow")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found
			log.Printf("%s: no config found", bold("flow"))
		} else {
			// Config found but another error was produced
			log.Fatalf("%s: Failed to read config file: %v", bold("flow"), err)
		}
	} else {
		log.Printf("%s: Using config file %s", bold("flow"), viper.ConfigFileUsed())
	}
	localconfig := viper.New()
	localconfig.SetConfigName("flow.yaml")
	localconfig.SetConfigType("yaml")
	localconfig.AddConfigPath(".")
	localconfig.SetEnvPrefix("flow")
	localconfig.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	localconfig.AutomaticEnv()
	if err := localconfig.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found
			log.Printf("no local config found")
		} else {
			log.Fatalf("%s: Failed to read local config file: %v", bold("flow"), err)
		}
	} else {
		log.Printf("%s: Using local config file %s", bold("flow"), localconfig.ConfigFileUsed())
	}

	for _, key := range localconfig.AllKeys() {
		viper.Set(key, localconfig.Get(key))
	}
	// log.Printf("container = %s", localc.GetString("resources.novoalign.container"))
}
