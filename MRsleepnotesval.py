from mrjob.job import MRJob
class MRsleepnotesval(MRJob):
    def mapper(self, _, line):
        dt=line.split(",")
        if not dt[0]=="Start":
            if len(dt)>5 and len(dt[5])>0:
                notes=dt[5].split(":")
                for x in notes:
                    yield x,1
            else:
                yield "no notes",1
            
            
        
    def reducer(self, key, values):
        yield key,sum(values)
        
if __name__ == '__main__':
    MRsleepnotesval.run()
