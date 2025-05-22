from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b\w+\b")

class WordCountMR(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    WordCountMR.run()
# to run  python word_python wordcount.py input.txt

