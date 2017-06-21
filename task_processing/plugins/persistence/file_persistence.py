import json

from pyrsistent import thaw
from pyrsistent import v

from task_processing.interfaces.event import Event
from task_processing.interfaces.event import json_deserializer
from task_processing.interfaces.event import json_serializer
from task_processing.interfaces.persistence import Persister


class FilePersistence(Persister):
    def __init__(self, output_file):
        self.output_file = output_file

    def read(self, task_id):
        acc = v()
        with open(self.output_file, 'r') as f:
            for line in f:
                parsed = json.loads(line, object_hook=json_deserializer)
                if parsed['task_id'] == task_id:
                    acc = acc.append(Event.create(parsed))
        return acc

    def write(self, event):
        with open(self.output_file, 'a+') as f:
            f.write("{}\n".format(json.dumps(
                thaw(event), default=json_serializer)))
