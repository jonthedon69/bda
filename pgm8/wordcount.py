from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b\w+\b")

class WordCountMR(MRJob):

    def mapper(self, _, line):
        """Emit each word with a count of 1"""
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer(self, word, counts):
        """Sum up counts for each word"""
        yield word, sum(counts)

if __name__ == "__main__":
    WordCountMR.run()
    
#to run pgm 8 python wordcount.py input.txt

