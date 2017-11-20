import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/')
import re
import sys
import json
import numpy as np
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from numpy import  pi

argvs = sys.argv
argc = len(argvs)

#text = argvs[1]

#if (argc != 2):
#    print('Usage: # python %s text' % argvs[0])
#    quit()


fig = plt.figure()
ax = fig.add_subplot( 1, 1, 1 )
ax.set_title( 'cpu_usage' )


timestamp = []
memory_usage = []

f = open(os.path.dirname(os.path.abspath(__file__))+'/resource_log/cpu_log')

line = f.readline()

while line:
    pattern = r"^(\d+):(\d+)$"
    matchOB = re.findall(pattern,line)
    timestamp.append( int(matchOB[0][0]) )
    memory_usage.append( int(matchOB[0][1]) )
    line = f.readline()

f.close

init_time = timestamp[0]

i = 0;
for time in timestamp:
    timestamp[i] = time - init_time
    i = i + 1


color = 'b'

print(timestamp)
print(memory_usage)

ax.plot(timestamp,memory_usage,color,linestyle='dashed')

plt.show()



#while line:
#    pattern1 = r"^[.\d]+:.*"
#    matchOB1 = re.findall(pattern1,line)
#    if matchOB1 != []:
#        list = parser.line_parse(line)
#        cpu_related_list.append(list[0])
#        memory_related_list.append(list[1])


