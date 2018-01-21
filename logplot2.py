import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/')
import re
import math
import sys
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


def mega_unity(str):
    result = 0
    unit = str[-1]
    if unit == 'M':
        result = float(str[:-1])
    elif unit == 'K':
        result = float(str[:-1]) / 1000
    elif unit == 'B':
        result = float(str[:-1]) / 1000000
    else:
        exit(0)
    return result


#---------------------------parse_eventlog--------------

timestamp = []
memory_usage = []

root_time = 0



nodes = ['huscs008','huscs10']#'huscs009','huscs10']





_k = 1.5

argvs = sys.argv
argc = len(argvs)

text = argvs[1]

if (argc != 2):
    print('Usage: # python %s text' % argvs[0])
    quit()

f = open('/Users/kotoda/sp_logbase/EXElogs/'+argvs[1])
print(argvs[1])



line = f.readline()
temp = -1
exe_tmp = -1

#main
executor_list = {} #[exe][stage][task]
stage_list = {} # [stage][exe][task]

stage_info = []
task_list = [] 

init = 1
num_of_node = 0

task_detail = []
job_list = []
applog = {'app_start_time':0,'app_end_time':0}


while line:
    pattern_task = r"SparkListenerTaskEnd"
    pattern_job = r"(SparkListenerJobStart)|(SparkListenerJobEnd)"
    pattern_app = r"(SparkListenerApplicationStart)|(SparkListenerApplicationEnd)"
    pattern_stage = r"SparkListenerStageCompleted"

    match_task = re.search(pattern_task,line)
    match_job = re.search(pattern_job,line)
    match_stage = re.search(pattern_stage,line)
    match_app = re.search(pattern_app,line)


    if match_app:
        dict = json.loads(line)
        if dict['Event'] == 'SparkListenerApplicationStart':
            applog['app_start_time'] = dict['Timestamp']
            root_time = dict['Timestamp']                     # root time
            print('root_time:')
            print(root_time)
        if dict['Event'] == 'SparkListenerApplicationEnd':
            applog['app_end_time'] = dict['Timestamp']


    if match_task:
        dict = json.loads(line)
        task_info = dict['Task Info']
        exe_id = int(task_info['Executor ID'])
        Task_ID = task_info['Task ID']
        Task_exe_time = task_info['Finish Time'] - task_info['Launch Time']
        stage_ID = dict['Stage ID']
        _task = {'Timestamp':task_info['Launch Time'], 'Finishtime':task_info['Finish Time'], 'time':task_info['Launch Time']-root_time, 'Task_ID':Task_ID, 'Task_exe_time':Task_exe_time, 'Stage_ID':stage_ID, 'Host':task_info['Host'], 'Executor_ID':task_info['Executor ID'], 'Locality':task_info['Locality'], 'state':0, 'gc_nums':0}

        if temp != stage_ID:
            temp = stage_ID
#            task_detail.insert(stage_ID,{'num_of_task':0, 'sum_of_exetime':0, 'avg_stage':0, 'std_stage':0})
            stage_info.insert(stage_ID,{'Stage_ID':stage_ID, 'startTime':0, 'stage_exetime':0, 'endTime':0, 'abnormal_node':[], 'num_of_task':0, 'task_exetimes':np.array([]), 'avg_stage':0, 'std_stage':0})
            stage_list[stage_ID] = {}

            
        if exe_id not in executor_list:
            executor_list[exe_id] = []
        executor_list[exe_id].append(_task)

        if num_of_node < int(exe_id):
            num_of_node = int(exe_id)
        if exe_id not in stage_list[stage_ID]:
            stage_list[stage_ID][exe_id] = []


        stage_list[stage_ID][exe_id].append(_task)
#        task_detail[stage_ID]['num_of_task'] = task_detail[stage_ID]['num_of_task'] + 1
#        task_detail[stage_ID]['sum_of_exetime'] = task_detail[stage_ID]['sum_of_exetime'] + Task_exe_time
        stage_info[stage_ID]['num_of_task'] += 1
        stage_info[stage_ID]['task_exetimes'] = np.append(stage_info[stage_ID]['task_exetimes'],Task_exe_time)


    if match_job:
        dict = json.loads(line)
        jobID = dict['Job ID']
        if dict['Event'] == 'SparkListenerJobStart':
            job_list.insert(jobID,{'Job_ID':jobID,'job_start_time':dict['Submission Time'],'job_end_time':0})
        if dict['Event'] == 'SparkListenerJobEnd':
            job_list[jobID]['job_end_time'] = dict['Completion Time']

    if match_stage:
        dict = json.loads(line)
        stage_data = dict['Stage Info']
        stage_info[stage_ID]['startTime'] = stage_data['Submission Time']
        stage_info[stage_ID]['endTime'] = stage_data['Completion Time']
        stage_info[stage_ID]['stage_exetime'] = stage_data['Completion Time'] - stage_data['Submission Time']

    line = f.readline()

f.close


print("\n",'stage_list:')
for stage in stage_list.items():
    print('stage:'+str(stage[0]))
    for exe in stage[1].items():
        print('exe:'+str(exe[0]))
        for task in exe[1]:
            print(task)


#print("\n",'task_detail')
#for stage in task_detail:
#    print(stage)





#---------gclog----------

exe_gc_log = {}
ax_gc = {}
fig_gc = plt.figure()

fig_event = plt.figure()

memory_ax = fig_event.add_subplot( 2, 1, 1 )
cpu_ax = fig_event.add_subplot( 2, 1, 2 )
#event_ax = fig_event.add_subplot( 3, 1, 3 )
memory_ax.set_title( 'memory_usage(MB/s)' )
cpu_ax.set_title( 'cpu_usage(%/s)' )
#event_ax.set_title( 'task exe time (ms)' )

each_stage_list = {}
gc_list = {}
gc_rate = {}
for i in range(2):
    f = open('/Users/kotoda/sp_logbase/GClogs/'+argvs[1]+'/'+str(i)+'/stdout')
    ax_gc[i] = fig_gc.add_subplot(2,1,i+1)
    ax_gc[i].set_title('executor-'+str(i))
    gc_list[i] = []
    gc_rate[i] = []
    time = []
    eden = []
    mx_eden = []
    survivor = []
    heap = []
    mx_heap = []
    
    each_stage_list[i] = {}

    heap_list = []

    line = f.readline()
    while line:
        flag = 0
        print(line)
        pattern1 = r"^(.+): \[GC pause \(([\d\w ]+)\) \((\w+)\).*, ([\d.]+) secs\]" 
        pattern7 = r"^(.+): \[Full GC \(([\d\w ]+)\)  (.*), ([\d.]+) secs\]"
        matchOB1 = re.findall(pattern1,line)
        matchOB7 = re.findall(pattern7,line)
        if matchOB1 != [] or matchOB7 != []:
            if matchOB1 == []:
                matchOB1 = matchOB7
            print(matchOB1[0][0])
            src = matchOB1[0][0]
            t = src.replace('+0900', '000')
            stamp = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")
            time.append(stamp.timestamp()-root_time/1000) 
            gc_exe_time = matchOB1[0][3]
            time.append( (stamp.timestamp() + float(gc_exe_time))-root_time/1000 )

            if i not in exe_gc_log:
                exe_gc_log[i] = []
            exe_gc_log[i].append(int(stamp.timestamp()*1000))
            
            flag = 1
            line = f.readline()
            if matchOB1[0][1]=='G1 Humongous Allocation':
                ax_gc[i].axvline(x=stamp.timestamp()-root_time/1000,color='k',linestyle='dashed')
            if matchOB1[0][2]=='mixed':
                ax_gc[i].axvline(x=stamp.timestamp()-root_time/1000,color='k',linestyle='solid')

            while line:
                matchOB1 = re.findall(pattern1,line)
                matchOB7 = re.findall(pattern7,line)
                if matchOB1 != [] or matchOB7 != []:
                    break
                pattern2 = r"^   \[Eden: ([\d.\w]+)\(([\d.\w]+)\)->([\d.\w]+)\(([\d\w.]+)\) Survivors: ([\d\w.]+)->([\d\w.]+) Heap: ([\d\w.]+)\(([\d\w.]+)\)->([\d\w.]+)\(([\d\w.]+)\)\]"
                pattern3 = r"^Heap$"
                matchOB2 = re.findall(pattern2,line)
                matchOB3 = re.findall(pattern3,line)
                if matchOB2 != []:
                    print('b')
                    gc_list[i].append({'time':stamp.timestamp()-root_time/1000,'eden':mega_unity(matchOB2[0][0]),'mx_eden':mega_unity(matchOB2[0][1]),'survivor':mega_unity(matchOB2[0][4]),'heap':mega_unity(matchOB2[0][6]),'mx_heap':mega_unity(matchOB2[0][7])})
                    gc_list[i].append({'time':(stamp.timestamp() + float(gc_exe_time))-root_time/1000,'eden':mega_unity(matchOB2[0][2]),'mx_eden':mega_unity(matchOB2[0][3]),'survivor':mega_unity(matchOB2[0][5]),'heap':mega_unity(matchOB2[0][8]),'mx_heap':mega_unity(matchOB2[0][9])})
                    gc_rate[i].append({'timestamp':stamp.timestamp(),'time':stamp.timestamp()-root_time/1000,'exe_time':gc_exe_time,'eden':mega_unity(matchOB2[0][0])-mega_unity(matchOB2[0][2]),'survivor':mega_unity(matchOB2[0][4])-mega_unity(matchOB2[0][5]),'heap':mega_unity(matchOB2[0][6])-mega_unity(matchOB2[0][8]),'gc_rate':( mega_unity(matchOB2[0][6])-mega_unity(matchOB2[0][8]) )/mega_unity(matchOB2[0][6])})
                    eden.append(mega_unity(matchOB2[0][0]))
                    eden.append(mega_unity(matchOB2[0][2]))
                    mx_eden.append(mega_unity(matchOB2[0][1]))
                    mx_eden.append(mega_unity(matchOB2[0][3]))
                    survivor.append(mega_unity(matchOB2[0][4]))
                    survivor.append(mega_unity(matchOB2[0][5]))
                    heap.append(mega_unity(matchOB2[0][6]))
                    heap.append(mega_unity(matchOB2[0][8]))
                    mx_heap.append(mega_unity(matchOB2[0][7]))
                    mx_heap.append(mega_unity(matchOB2[0][9]))
                if matchOB3 != []:
                    break
                line = f.readline() 
        if flag == 0:
            line = f.readline()
    f.close

    
    exe_num = i
    tasks = sorted(executor_list[i], key=lambda x: x['Timestamp'])
    executor_list[i] = tasks

    x_axes_time = []
    y_axes_exe_time = []
    stage = 0
    gc_check_list = []
    each_stage_list[exe_num][stage] = {'Gcnum':0,'Task_num':0,'Gcrate':0}
    for task in tasks:
        x_axes_time.append(float(task['time'])/1000)
        y_axes_exe_time.append(float(task['Task_exe_time']))
        ax_gc[i].axvline(x=float(task['time'])/1000,color='c',alpha=0.2,linestyle='solid')
        ax_gc[i].axvspan(float(task['time'])/1000,( float(task['time'])+float(task['Task_exe_time']) )/1000,color='y',alpha=0.2)
        if task['Stage_ID'] != stage:
            each_stage_list[exe_num][stage]['Gcnum'] = len(gc_check_list)  
            gc_check_list = []
            stage = task['Stage_ID']
            each_stage_list[exe_num][stage] = {'Gcnum':0,'Task_num':0, 'Gcrate':0}

        for gctime in exe_gc_log[exe_num]:

            if int(task['Timestamp']) < gctime and gctime < int(task['Finishtime']):       #gcrate
                task['gc_nums'] = task['gc_nums'] + 1
                if gctime not in gc_check_list:
                    gc_check_list.append(gctime)
            if int(task['Finishtime']) < gctime:
                break

        each_stage_list[exe_num][stage]['Task_num'] = each_stage_list[exe_num][stage]['Task_num'] + 1

#        if task['gc_nums'] > 11:   
#            for gctime in exe_gc_log[exe_num]:
#                if int(task['Timestamp']) < gctime and gctime < int(task['Finishtime']):        
#                    ax_gc[i].axvline(x=(gctime-root_time)/1000,color='c',linestyle='solid')

    each_stage_list[exe_num][stage]['Gcnum'] = len(gc_check_list)


 #   print('task_list:')
 #   for task in executor_list[i]:
 #       print(task)
 #   print('gc_check_list:')
 #   print(gc_check_list)
    print('each_stage_list:')
    for stage in each_stage_list[exe_num]:
        print(stage)

    if exe_num == 0:
            c = 'b'
    elif exe_num == 1:
            c = 'r'
    elif exe_num == 2:
            c = 'g'
    else:
            c = 'k'



    ax_gc[i].plot(time,eden,color='g')
    ax_gc[i].fill_between(time,mx_eden,0,color='g',alpha=0.2)
    ax_gc[i].plot(time,survivor,color='b')
    ax_gc[i].plot(time,heap,color='r')
    ax_gc[i].fill_between(time,mx_heap,0,color='r',alpha=0.2)
    

    print('x_axes_time:')
    print(x_axes_time)
    print('y_axes_exe_time:')
    print(y_axes_exe_time)
#    event_ax.bar(x_axes_time,y_axes_exe_time,edgecolor=c,color=c,width=0.01)  



for exe in gc_list.items():
    print('gc_list:')
    print(exe[0])
    for gc in exe[1]:
        print(gc)
    
#plt.xlim(xmin=0.0)
#plt.ylim(ymin=0.0)    

#------------Aggregation----------------
#all_task_num = 0
#each_stage_list = {}
#for j in range(len(executor_list)):
#    all_task_num = all_task_num + len(executor_list[j])
#    for task in executor_list[j]:
        
                                          
        
#----------------------------

#plt.show()


#=============================================
# print('task_list:')
#    for exe in executor_list.items():
#        for task in exe[1]:
#            print(task)


#fig = plt.figure()

#memory_ax = fig.add_subplot( 3, 1, 1 )
#cpu_ax = fig.add_subplot( 3, 1, 2 )
#event_ax = fig.add_subplot( 3, 1, 3 )
#memory_ax.set_title( 'memory_usage(MB/s)' )
#cpu_ax.set_title( 'cpu_usage(%/s)' )
#event_ax.set_title( 'task exe time (ms)' )
#event_ax.bar(x_axes_time,y_axes_exe_time,edgecolor=c,color=c,width=0.01)
#   cpu_ax.plot(timestamp,cpu_usage,color)
#i = i+1




# -------------memory plot-----------------

cpu_rate = {}
memory_rate = {}

for node in nodes:

    if node == 'huscs008':
        color = 'r'
    elif node == 'huscs009':
        color = 'g'
    elif node == 'huscs10':
        color = 'b'
    else:
        color = 'k'


    memory_f = open(os.path.dirname(os.path.abspath(__file__))+'/Cglogs/'+argvs[1]+'/'+node+'/0/memory_log')
    line = memory_f.readline()

    timestamp = []
    memory_usage = []
    
#    root_init = 1
    while line:
        pattern = r"^(\d+):(\d+)$"
        matchOB = re.findall(pattern,line)
#        print(matchOB[0][0])
        timestamp.append( int(matchOB[0][0])/1000 )
        memory_usage.append( float(matchOB[0][1])/1000000 )
        if node not in memory_rate:
            memory_rate[node] = []
        memory_rate[node].append({'timestamp':int(int(matchOB[0][0])/1000),'time':int( (int(matchOB[0][0])-root_time )/1000),'memory_use': float(matchOB[0][1])/1000000})
        line = memory_f.readline()

    memory_f.close

    i = 0;
    for time in timestamp:
        timestamp[i] = time - int(root_time)/1000
        i = i + 1


    memory_ax.plot(timestamp,memory_usage,color) 
    


# -------------cpu plot-----------------


    cpu_f = open(os.path.dirname(os.path.abspath(__file__))+'/Cglogs/'+argvs[1]+'/'+node+'/0/cpu_log')
    line = cpu_f.readline()

    timestamp = []
    cpu_usage = []

    while line:
        pattern = r"^(\d+):(\d+)$"
        matchOB = re.findall(pattern,line)
        timestamp.append( int(matchOB[0][0])/1000 )
        cpu_usage.append( float(matchOB[0][1])/10000000 )
        if node not in cpu_rate:
            cpu_rate[node] = {}
        cpu_rate[node][str(int((float(matchOB[0][0])-root_time )/1000))] = {'timestamp':int(int(matchOB[0][0])/1000), 'time':int((int(matchOB[0][0])-root_time )/1000), 'cpu_use': float(matchOB[0][1])/10000000, 'state': 0, 'solo':0}
        if float(matchOB[0][1])/10000000 >= 5:
            cpu_rate[node][str(int((float(matchOB[0][0])-root_time )/1000))]['state'] = 1
        line = cpu_f.readline()

    cpu_f.close

    i = 0;
    for time in timestamp:
        timestamp[i] = time - int(root_time)/1000
        i = i + 1

        

    cpu_ax.plot(timestamp,cpu_usage,color)


#plt.show()


print('\nmemory_rate:')
for exe in memory_rate.items():
    print(exe[0])
    for gc in exe[1]:
        print(gc)

#while line:
#    pattern1 = r"^[.\d]+:.*"
#    matchOB1 = re.findall(pattern1,line)
#    if matchOB1 != []:
#        list = parser.line_parse(line)
#        cpu_related_list.append(list[0])
#        memory_related_list.append(list[1])


#-----------time_aggregation-----------
temp = cpu_rate[node].items()
print(cpu_rate[node])
for i in range(len(temp)): 
    sum = 0
    for exe in nodes:
        if str(i) in cpu_rate[exe] and cpu_rate[exe][str(i)]['state'] == 1:
            cpu_rate[exe][str(i)]['solo'] = 1
            sum = sum + 1
            print(sum)
    if sum > 1:
        for exe in nodes:
            if str(i) in cpu_rate[exe]:
                cpu_rate[exe][str(i)]['solo'] = 0
    

print('\ncpu_rate:')
for exe in cpu_rate.items():
    for t in exe[1].items():
        print(t[0])
        print(t[1])


#print("\n",'stage_list:')
#for stage in stage_list.items(): #each stages
#    stage_num = stage[0]
#    start_time = stage_info[stage_num]['startTime']
#    end_time = stage_info[stage_num]['endTime']
#    for exe in stage[1].items(): #each nodes
#        for task in exe[1]: #each tasks
#            print(task)
k = 0.2





#------------stage_aggregation-------------
for stage in stage_info:
    stage['avg_stage'] = np.mean(stage['task_exetimes'])
    stage['std_stage'] = np.std(stage['task_exetimes'])
    stage_id = stage['Stage_ID']
    start_time = stage['startTime']/1000
    end_time = stage['endTime']/1000
    exe_time = end_time - start_time
#    if exe_time < 5:
#        continue
    print('stage:',stage_id,' exe_time:',exe_time)
    for exe in nodes:
#        gcrate_temp = 0
#        for cpu_log in cpu_rate[exe].items():
#            print(end_time,':',cpu_log[1]['timestamp'])
#            if end_time < cpu_log[1]['timestamp']:
#                break
#            if start_time < cpu_log[1]['timestamp']: 
#                if cpu_log[1]['solo'] == 1:
#                    abnormal_time = abnormal_time + 1
        node_i = nodes.index(exe)
        try:
            for task in stage_list[stage_id][node_i]:
                print('exe:',node_i)
                print(task['Task_exe_time'], ':', stage_info[stage_id]['avg_stage'] + 1.5 * stage_info[stage_id]['std_stage'])
                if task['Task_exe_time'] > stage_info[stage_id]['avg_stage'] + 1.5 * stage_info[stage_id]['std_stage']:
                    task['state'] = 1
                    stage['abnormal_node'].append(exe)
        except KeyError:
            print(exe,' not have task in stage_',node_i)
        for gc in gc_rate[node_i]:
            if stage_id in each_stage_list[node_i]:
                if each_stage_list[node_i][stage_id]['Gcnum'] != 0:
                    if end_time < gc['timestamp']:
                        break
                    if start_time < gc['timestamp']:
                        print('gcrate:',gc)
                        each_stage_list[node_i][stage_id]['Gcrate'] += gc['gc_rate']
        try:
            if each_stage_list[node_i][stage_id]['Gcnum'] != 0:
                each_stage_list[node_i][stage_id]['Gcrate'] /= each_stage_list[node_i][stage_id]['Gcnum']
#                each_stage_list[node_i][stage_id]['Gcrate'] /= each_stage_list[node_i][stage_id]['Gcnum']
        except KeyError:
            continue
            
#        print('stage:',stage['Stage_ID'],' node:',exe,' abnormal_rate:',abnormal_time/exe_time) 
#        if abnormal_time/exe_time > k :
#            stage['abnormal_node'].append(exe)


        
print("\n",'stage_list:')                                                                                                                                                                                                                                                                                                                                                                                                               
for stage in stage_list.items(): #each stages                                                                                                                                                                                                                                                                                                                                                                                           
    print(stage[0])
    for exe in stage[1].items(): #each nodes                                                                                                                                                                                                                                                                                                                                                                                            
        print(exe[0])
        for task in exe[1]: #each tasks                                                                                                                                                                                                                                                                                                                                                                                                 
            print(task)                          

print("\n",'stage_info')
for stage in stage_info:
    stage['avg_stage'] = np.mean(stage['task_exetimes'])
    stage['std_stage'] = np.std(stage['task_exetimes'])
    print(stage)

print("\neach_stage_list:")
for exe in each_stage_list.items():
    print('exe:',exe[0])
    for stage in exe[1].items():
        print('stage:',stage[0])
        print(stage[1])
    

plt.show() 
