from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    def configure_options(self):
        super(MRWordFrequencyCount, self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations', default=3, type='int',
            help='number of iterations to run')

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return ([self.mr(mapper=self.mapper, reducer=self.reducer)] *
            self.options.iterations)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
