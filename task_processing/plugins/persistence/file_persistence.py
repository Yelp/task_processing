import json
import os
from task_processing.interfaces.persistence.reader import Reader
from task_processing.interfaces.persistence.writer import Writer

class FilePersistence(Reader, Writer):
    def __init__(self, output_file):
        self.output_file = output_file
        pass

    def read(self, task_id):
        pass

    def write(self, event):
        with open(self.output_file, 'a+') as f:
            f.write("{}\n".format(json.dumps(event.serialize())))
