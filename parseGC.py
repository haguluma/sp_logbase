import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/')

import re
import sys
import parse as parser

import json
import numpy as np
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
from numpy import  pi


argvs = sys.argv
argc = len(argvs)

text = argvs[1]

if (argc != 2):
    print('Usage: # python %s text' % argvs[0])
    quit()


fig = plt.figure()
ax_young = fig.add_subplot( 2, 1, 1 )
ax_young.set_title( 'Young Generation' )
ax_old = fig.add_subplot( 2, 1, 2 )
ax_old.set_title( 'Old Generation' )


for i in range(3):                                 
    f = open(os.path.dirname(os.path.abspath(__file__))+'/GClogs/'+argvs[1]+'/'+str(i)+'/stdout') #とりあえずノードは3で仮定

    cpu_related_list = []
    memory_related_list = []

    line = f.readline()

    while line:
        pattern1 = r"^[.\d]+:.*"
        matchOB1 = re.findall(pattern1,line)
        if matchOB1 != []:
#            print('OB1:',matchOB1)
            list = parser.line_parse(line)
            cpu_related_list.append(list[0])
            memory_related_list.append(list[1])
        line = f.readline()                    
    f.close
       
    for node in cpu_related_list:
        print(node)

    print('\n','--------------------')

    for node in memory_related_list:
        print(node)

    print('----------------------------\n\n')


    x_young = []
    x_young_heap = []
    x_old = []
    x_old_heap = []

    y_young = []
    y_young_heap = []
    y_old = []
    y_old_heap = []



    for node in memory_related_list:
        if node['PSYoungGen'] == 1:
            x_young.append(float(node['timestamp']))
            x_young.append(float(node['timestamp']))
            x_young_heap.append(float(node['timestamp']))
            y_young.append(float(node['before_young_obj']))
            y_young.append(float(node['after_young_obj']))
            y_young_heap.append(float(node['young_heap_space']))
        if node['ParOldGen'] == 1:
            x_old.append(float(node['timestamp']))
            x_old.append(float(node['timestamp']))
            x_old_heap.append(float(node['timestamp']))
            y_old.append(float(node['before_old_obj']))
            y_old.append(float(node['after_old_obj']))
            y_old_heap.append(float(node['old_heap_space']))
    
    color = 'k'
    if i == 0:
        color = 'r'
    if i == 1:
        color = 'b'
    if i == 2:
        color = 'g'

    ax_young.plot(x_young,y_young,color,linestyle='dashed')
    ax_young.plot(x_young_heap,y_young_heap,color,linestyle='solid')
    ax_old.plot(x_old,y_old,color,linestyle='dashed')
    ax_old.plot(x_old_heap,y_old_heap,color,linestyle='solid')

plt.show()
