User defines a `Workflow(q flow.Queue) error` function that populates the
queue with Taskers (this is a bad name that needs changing).

Internally `Tasker` are transformed into `job` structs. The `job`s are added
to a `flow.graph` (again poorly named, it isn't a graph).

`graph.Process` is the function that actually runs the worfklow.

Currently, each runner is creating a temp directory and populating it with the scripts to run. This should be handled outside of the runner so that there is no duplication. The runner should only be responsible for running the job not the setup of the execution context.