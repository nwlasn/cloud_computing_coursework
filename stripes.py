
from mrjob.job import MRJob
import re

words = re.compile(r"[\w']+")


class MRStripes(MRJob):

    
    def mapper(self, _, line):
        line = line.split('"',1)[1]
        line = [i.lower() for i in words.findall(line)]
        
        for w, u in zip(line, line[1:]):
            d = dict()
            if u in d:
                d[u] += 1
            else:
                d[u] = 1
            yield w, d
            
            
    def reducer(self, word, dictionaries):
        di = dict()
        
        for dictionary in dictionaries:
            di = {k: di.get(k,0) + dictionary.get(k,0) for k in set(di) | set(dictionary)}
            
        w_count = sum(di.values())
        
        for cond in di:
            di[cond] = di[cond]/w_count 
        
        if word == 'my':
            yield word, [i[0] for i in sorted(di.items(), key = lambda kv:(kv[1], kv[0]), reverse = True)[0:10]]
            


if __name__ == '__main__':
    MRStripes.run()