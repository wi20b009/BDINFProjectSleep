from mrjob.job import MRJob
class MRsleepavg(MRJob):
    def mapper(self, _, line):
        dt=line.split(",")
        if not dt[0]=="Start":
            year=dt[0].split("-")[0]
            if len(dt)>=4:
                time=dt[3].split(":")
                h=int(time[0])
                m=int(time[1])/60
                yield int(year), h+mnoted
            
        
    def reducer(self, key, values):
        c=0
        s=0
        for x in values:
            s=s+x
            c=c+1
        avg=s/c
        yield key,avg
        
if __name__ == '__main__':
    MRsleepavg.run()
