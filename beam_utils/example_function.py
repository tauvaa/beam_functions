import apache_beam as beam


class CountLetter(beam.DoFn):
    def __init__(self, letter):
        self.letter = letter

    def process(self, element, *args, **kwargs):
        counter = 0
        for c in element[1]:
            if c == self.letter:
                counter += 1
        yield counter


def extra_pipe():
    return (
        beam.ParDo(CountLetter("h"))
        | beam.Filter(lambda x: x > 0)
        | beam.Map(print)
    )


if __name__ == "__main__":
    with beam.Pipeline(options=beam.pipeline.PipelineOptions([])) as pipe:
        data = beam.Create(["test", "something"])
        pipe | data | extra_pipe()
