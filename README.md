# Generic task processing

Interfaces and shared infrastructure for generic task processing @ Yelp

# Structure

## /interfaces

### Event

### Runner

### TaskExecutor

## /plugins

### mesos

Implements all required interfaces to talk to Mesos deployment.

#### Configuration options

- authentication\_principal Mesos principal
- credential\_secret\_file path to file containing Mesos secret
- mesos\_address host:port to connect to Mesos cluster
- event_translator a fucntion that maps Mesos-specific events to `Event` objects

## /runners

Runners provide specific concurrency semantics and are supposed to be
platform independent.

### Sync

Running a task is a blocking operation.

### Async

Provide callbacks for different events in tasks' lifecycle.

### Promise/Future

Running a task returns future object.

### Subscription

Provide a queue object and receive all events in there.

# How do i try it?

There are some examples in `/examples`. To run them you need docker and
docker-compose.

    cd examples/cluster/
    docker-compose run playground examples/sync.py
