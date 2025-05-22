from mrjob.job import MRJob

class MovieTagAnalysis(MRJob):
    def mapper(self, _, line):
        try:
            movie_id, tag = line.split(',')
            movie_id = movie_id.strip()
            tag = tag.strip()
            yield movie_id, tag
        except ValueError:
            pass  # Skip malformed lines

    def reducer(self, movie_id, tags):
        yield movie_id, list(tags)

if __name__ == "__main__":
    MovieTagAnalysis.run()
    
#to run pg4 python movie_tag_analysis.py movie_tags.csv

