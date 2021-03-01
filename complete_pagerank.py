
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

numbers = re.compile(r"[0-9]+")



class MRPageRank(MRJob):
    

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                       reducer=self.reducer1),
                MRStep(mapper=self.mapper2,
                       reducer=self.reducer2),
                MRStep(reducer=self.reducer3)]
                   
        
    def mapper1(self, _, line):
        if '#' in line:
            pass
        else:
            line = numbers.findall(line)
            if line[0] != line[1]:
                yield int(line[0]), int(line[1])
            else:
                pass
             
                
    def reducer1(self, node, outlinks):
        nodes = 75879
        adjacency_list = []
        
        for outlink in outlinks:
            adjacency_list.append(int(outlink))
            
        initial_pagerank = str(1/nodes)
        outlinks = 'outlinks:' + str(adjacency_list)
        node = str(node)
        values = node + ' ' + initial_pagerank + ' ' + outlinks
        yield node, values       
          
            
    def mapper2(self, _, values):
        adjacency_list = values.split(' outlinks:')
        outlinks = adjacency_list[1][1:(len(adjacency_list[1])-1)].split(', ')
        pagerank = float(adjacency_list[0].split(' ')[1])
        pagerank /= len(outlinks)
        pr = 0
        node = adjacency_list[0].split(' ')[0]
        values = str(pr) + ' outlinks:' + str(adjacency_list[1])
        yield int(node), values
        
        for n in range(len(outlinks)):
            outlink = outlinks[n]
            pr = str(pagerank)
            node = outlink
            values = pr
            yield int(node), values
          
        
    def reducer2(self, node, values):
        nodes = 75879
        page_rank_sum = 0
        
        for v in values:
            if ' outlinks:' in str(v):
                data = v.split(' outlinks:')
                outlinks = data[1]
            else:
                page_rank_sum += float(v)
                outlinks = '[]'
                
        page_rank_sum = 0.15/nodes + 0.85 * page_rank_sum
        
        if len(outlinks) > 2:
            result = str(node) + ' ' + str(page_rank_sum) + ' outlinks:' + outlinks
            yield None, result
        else:
            result = str(node) + ' ' + str(page_rank_sum) + ' outlinks:[]'
            yield None, result
            
            
    def reducer3(self, _, values):
        maxi = []
        pageranks = {}
        
        for value in values:
            val = value.split(' ')
            pageranks[float(val[1])] = int(val[0])
            
        for _ in range(10):
            top = max(pageranks)
            maxi.append(pageranks[top])
            del pageranks[top]
            
        yield 'top10', maxi
        
    
            
            
if __name__ == '__main__':
    MRPageRank.run()