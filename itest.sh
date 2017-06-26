#!/bin/bash

set -eux

examples/sync.py
examples/async.py
examples/subscription.py
examples/file_persistence.py
examples/dynamo_persistence.py
