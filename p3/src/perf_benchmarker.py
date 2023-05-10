import argparse
import os
from subprocess import run
import time

# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument
parser.add_argument("-t", "--topicId", nargs='?', const=1, type=int, default=1, help = "Topic Id for publisher")
parser.add_argument("-b", "--brokerId", nargs='?', const=1, type=int, help = "Broker Id for publisher")

# Read arguments from command line
args = parser.parse_args()

if args.topicId:
    print("Topic Id for publisher: % s" % args.topicId)
if args.brokerId:
    print("Broker Id for publisher: % s" % args.brokerId)

def main():
    # print(args.topicId)
    # print(args.brokerId)
    FNULL = open(os.devnull, 'w')
    

    t_start = t = time.perf_counter_ns()
    count = 0
    throughput = []
    brokerId = 0
    for i in range(100):
        cmd = ["./build/publisher", "-t", str(args.topicId), "-b", str(brokerId), "-l 8"]
        p = run(cmd, stdout=FNULL, stderr=FNULL)
        if(p.returncode == 255 or p.returncode == -1):
            print(f'{i} returncode: {p.returncode}')
        else: 
            brokerId = p.returncode
            # print(f'{i} brokerId: {brokerId}')
        
        t_s = time.perf_counter_ns()
        if(t_s - t > 200000000 ):
            tput = (i-count)*1e9/(t_s-t)
            print(tput)
            throughput.append(tput)
            t=time.perf_counter_ns()
            count=i
    print(throughput)

if __name__ == "__main__":
    main()