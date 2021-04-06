package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"plugin"

	"github.com/jje42/flow"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "flow",
		Short: "",
		Long:  "",
		Run:   myMain,
	}
)

// To ensure the flow's config file is read before anything else is done.
var _ flow.Queue

func myMain(cmd *cobra.Command, args []string) {
	fn := args[0]
	err := runWorkflow(fn)
	if err != nil {
		log.Fatal(err)
	}
}

func runWorkflow(fn string) error {
	dir, err := ioutil.TempDir("", "workflow")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)
	err = copyFile(fn, fmt.Sprintf("%s/workflow.go", dir))
	if err != nil {
		return fmt.Errorf("failed to copy workflow to temp directory: %v", err)
	}

	// Creating go.mod
	err = createGoMod(dir)
	if err != nil {
		return err
	}

	log.Printf("Compiling workflow\n")
	cmdl := exec.Command("go", "build", "-buildmode=plugin", "workflow.go")
	cmdl.Dir = dir
	out, err := cmdl.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to compile workflow: %v\n%v", err, string(out))
	}

	p, err := plugin.Open(fmt.Sprintf("%s/workflow.so", dir))
	if err != nil {
		return fmt.Errorf("failed to open plugin: %v", err)
	}
	pWorkflow, err := p.Lookup("Workflow")
	if err != nil {
		return fmt.Errorf("failed to find Workflow function in plugin: %v", err)
	}
	workflowFunc, ok := pWorkflow.(func(*flow.Queue))
	if !ok {
		return fmt.Errorf("workflow func found, but it's type is %T", pWorkflow)
	}
	queue := &flow.Queue{}
	workflowFunc(queue)
	err = queue.Run()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
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

// Is this really necessary?
func createGoMod(dir string) error {
	// Creating go.mod
	w, err := os.Create(fmt.Sprintf("%s/go.mod", dir))
	if err != nil {
		return fmt.Errorf("failed to create go.mod: %v", err)
	}
	defer w.Close()
	w.WriteString("module github.com/jje42/workflow\n")
	w.WriteString("go 1.15\n")
	w.WriteString("replace github.com/jje42/flow => /home/ellisjj/scratch/flow\n")
	return nil
}
