
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

words = re.compile(r"[\w']+")


class MRPairs(MRJob):
    
    
    SORT_VALUES = True
    
    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

    
    def mapper(self, _, line):
        line = [i.lower() for i in words.findall(line.split('"',1)[1])]
        for w, u in zip(line, line[1:]):
            yield (w, '*'), 1
            yield (w, u), 1
            
            
    def combiner(self, pair, counts):
        yield pair, sum(counts)
         
        
    def reducer1(self, pair, counts):   
        w, u = pair
        if u == '*':
            yield w, ('A', sum(counts))
        else:
            yield w, ('B', (u, sum(counts)))
         
        
    def reducer2(self, word, uc):
        counts = 0
        lst = []
        for letter, data in uc:
            if letter == 'A':
                counts = data
            else:
                u, cnts = data
                if word == 'my':
                    lst.append((u, cnts/counts))  
                    
        if word == 'my':
            yield 'my', [i[0] for i in sorted(lst, key=lambda tup: (tup[1], tup[0]), reverse = True)[0:10]]
            
            

if __name__ == '__main__':
    MRPairs.run()