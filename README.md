# Task Processing

Interfaces and shared infrastructure for generic task processing (also known as `taskproc`) at Yelp.

## Developer Setup

### Pre-requisites

+ [Python 3.8](https://www.python.org/downloads/)
+ [Virtualenv](https://virtualenv.pypa.io/en/stable/installation/)

### Running tests

From the root of the repository, run:

    make

## Repository Structure

### /interfaces

#### Event

#### Runner

#### TaskExecutor

### /plugins

Plugins can be chained to create a task execution pipeline with more than one property.

#### Kubernetes
Implements all required interfaces to talk to Kubernetes. This plugin uses [kubernetes-client](https://github.com/kubernetes-client/python) to communicate with Kubernetes.

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
