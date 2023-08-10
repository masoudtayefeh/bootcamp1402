import json
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback


class ModJSON(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        data = json.loads(text)

        final = {}
        for item, value in enumerate(data):
            for column in data[item]:
                final[column] = data[item][column]

        outputStream.write(bytearray(json.dumps(final, indent=4).encode('utf-8')))


flowFile = session.get()
if (flowFile != None):
    flowFile = session.write(flowFile, ModJSON())
    flowFile = session.putAttribute(flowFile, "filename",
                                    flowFile.getAttribute('filename'))
session.transfer(flowFile, REL_SUCCESS)
session.commit()