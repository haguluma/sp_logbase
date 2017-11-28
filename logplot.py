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

timestamp = []
memory_usage = []

memory_ax = fig.add_subplot( 2, 1, 1 )
cpu_ax = fig.add_subplot( 2, 1, 2 )

memory_ax.set_title( 'memory_usage(MB/s)' )
cpu_ax.set_title( 'cpu_usage(ns/s)' )

nodes = ['huscs008','huscs009','huscs10']


for node in nodes:

    if node == 'huscs008':
        color = 'r'
    elif node == 'huscs009':
        color = 'b'
    elif node == 'huscs10':
        color = 'g'
    else:
        color = 'b'


    memory_f = open(os.path.dirname(os.path.abspath(__file__))+'/Cglogs/memory/'+node+'/memory_log')
    line = memory_f.readline()

    timestamp = []
    memory_usage = []

    while line:
        pattern = r"^(\d+):(\d+)$"
        matchOB = re.findall(pattern,line)
        timestamp.append( int(matchOB[0][0])/1000000000 )
        memory_usage.append( int(matchOB[0][1])/1000000 )
        line = memory_f.readline()

    memory_f.close

    init_time = timestamp[0]

    i = 0;
    for time in timestamp:
        timestamp[i] = time - init_time
        i = i + 1


    memory_ax.plot(timestamp,memory_usage,color) 
    


# -------------cpu plot-----------------


    cpu_f = open(os.path.dirname(os.path.abspath(__file__))+'/Cglogs/cpu/'+node+'/cpu_log')
    line = cpu_f.readline()

    timestamp = []
    cpu_usage = []

    while line:
        pattern = r"^(\d+):(\d+)$"
        matchOB = re.findall(pattern,line)
        timestamp.append( int(matchOB[0][0])/1000000000 )
        cpu_usage.append( int(matchOB[0][1])/1000000000 )
        line = cpu_f.readline()

    cpu_f.close

    init_time = timestamp[0]

    i = 0;
    for time in timestamp:
        timestamp[i] = time - init_time
        i = i + 1


    cpu_ax.plot(timestamp,cpu_usage,color)







plt.show()



#while line:
#    pattern1 = r"^[.\d]+:.*"
#    matchOB1 = re.findall(pattern1,line)
#    if matchOB1 != []:
#        list = parser.line_parse(line)
#        cpu_related_list.append(list[0])
#        memory_related_list.append(list[1])


