# Task Processing
[![Build Status](https://travis-ci.org/Yelp/task_processing.svg?branch=master)](https://travis-ci.org/Yelp/task_processing)

Interfaces and shared infrastructure for generic task processing (also known as `taskproc`) at Yelp.

## Developer Setup

### Pre-requisites

+ [Docker](https://www.docker.com/get-docker)
+ [Python 3.6](https://www.python.org/downloads/)
+ [Virtualenv](https://virtualenv.pypa.io/en/stable/installation/)

### Running examples

[hello-world.py](/examples/hello-world/py) is a very simple annotated example that launches a task to echo `hello world`. From the root of the repository, run:

    docker-compose -f examples/cluster/docker-compose.yaml \
      run playground examples/hello-world.py

This will bring up a single master, single agent Mesos cluster using [Docker Compose](https://docs.docker.com/compose/) and launch a single task which will print "hello world" to the sandbox's stdout before terminating.

Other examples available include:
+ async.py
Example of the [async](#async) task runner.

+ dynamo_persistence.py  
Example that shows how task events may be persisted to [DynamoDB](https://aws.amazon.com/dynamodb) using the `stateful` plugin.

+ file_persistence.py  
Example that shows how task events may be persisted to disk using the `stateful` plugin.

+ promise.py  
Example that shows how the [promise/future](#Promise/Future) task runner (not yet implemented) may be used.

+ subscription.py  
Example of the [subscription](#subscription) task runner.

+ sync.py  
Brief example using the [sync](#sync) task runner.

+ timeout.py
Example that shows how to timeout a task execution using the `timeout` plugin.

+ retry.py
Example that shows how to retry a task on failure using the `retry` plugin.

+ task_logging.py
Example that shows how to fetch task logs from Mesos agents using the `logging` plugin.

### Running tests

From the root of the repository, run:

    make

## Repository Structure

### /interfaces

#### Event

#### Runner

#### TaskExecutor

### /plugins

Plugins can be chained to create a task execution pipeline with more than one property. Please refer to persistence/retry/timeout examples.

#### mesos
Implements all required interfaces to talk to Mesos deployment. This plugin uses [PyMesos](https://github.com/douban/pymesos) to communicate with Mesos.

#### timeout
Implements an executor to timeout task execution.

#### retrying
Implements an executor to retry task execution upon failure.

#### logging
Implements an executor to retrieve task logs from Mesos agents. Note that it has to be the immediate upstream executor of the mesos executor.

##### Configuration options

- authentication\_principal Mesos principal
- credential\_secret\_file path to file containing Mesos secret
- mesos\_address host:port to connect to Mesos cluster
- event_translator a fucntion that maps Mesos-specific events to `Event` objects

#### stateful

TODO: documentation

### /runners

Runners provide specific concurrency semantics and are supposed to be
platform independent.

#### Sync

Running a task is a blocking operation. `sync` runners block until the running task has completed or a `stop` event is received.

#### Async

Provide callbacks for different events in tasks' lifecycle. `async` runners allow tasks to specify one or more EventHandlers which consist of predicates and callbacks. Predicates are evaluated when an update is received from the task (e.g. that it has terminated and whether or not it has succeded) and if the predicate passes, the callback is called.

#### Promise/Future

Running a task returns future object.

#### Subscription

Provide a queue object and receive all events in there.
