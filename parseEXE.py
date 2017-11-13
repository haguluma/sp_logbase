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

_k = 1.5

argvs = sys.argv
argc = len(argvs)

text = argvs[1]

if (argc != 2):
    print('Usage: # python %s text' % argvs[0])
    quit()

f = open('/Users/kotoda/testpy2/EXElogs/'+argvs[1])
print(argvs[1])
                                                                                                                                                                                                              
line = f.readline()


#メモ
#・task_listとexe_infoは連動するので注意
#
#
#
#


#タスク関連変数
temp = -1
init = 1
num_of_node = 0
task_list = []
task_detail = []
task_exetime_stage_index = []
exe_info = []
init_exe_info = []
tasklog_list = []
job_list = []
applog = {'app_start_time':0,'app_end_time':0}
stage_list = []

while line:
    pattern_task = r"SparkListenerTaskEnd" #"(SparkListenerTaskStart)|(SparkListenerTaskEnd)|(SparkListenerJobStart)|(SparkListenerJobEnd)"
    pattern_job = r"(SparkListenerJobStart)|(SparkListenerJobEnd)"
    pattern_app = r"(SparkListenerApplicationStart)|(SparkListenerApplicationEnd)"
    pattern_stage = r"SparkListenerStageCompleted"

    match_task = re.search(pattern_task,line)
    match_job = re.search(pattern_job,line)
    match_stage = re.search(pattern_stage,line)
    match_app = re.search(pattern_app,line)

#タスク関連
    if match_task:
        dict = json.loads(line)
        task_info = dict['Task Info']
        Task_ID = task_info['Task ID']
        Task_exe_time = task_info['Finish Time'] - task_info['Launch Time']
        stage_ID2 = dict['Stage ID']
        _task = {'Timestamp':task_info['Launch Time'],'time':0,'Task_ID':Task_ID,'Task_exe_time':Task_exe_time,'Stage_ID':stage_ID2,'Host':task_info['Host'],'Executor_ID':task_info['Executor ID'],'Locality':task_info['Locality'],'state':0}
        task_list.insert(Task_ID,_task)

        #ステージごとに実行時間をリストにして保存
        if temp != stage_ID2 :
            temp = stage_ID2
            #init = 1 # initialタスク判定(仮)
            task_detail.insert(stage_ID2,{'num_of_task':0,'sum_of_exetime':0,'avg_stage':0,'std_stage':0})
            exe_info.insert(stage_ID2,{})
            init_exe_info.insert(stage_ID2,{})
            temp_array = np.array([])
            task_exetime_stage_index.insert(temp,temp_array)
        
        exe_id = task_info['Executor ID']
        if num_of_node < int(exe_id):
            num_of_node = int(exe_id)
        if exe_id not in exe_info[stage_ID2]:
            exe_info[stage_ID2][exe_id] = []
            init_exe_info[stage_ID2][exe_id] = []

            
        #initialタスクグルーピング
        #if init != 1:
        exe_info[stage_ID2][exe_id].append(_task)
        #else:
        #    init_exe_info[stage_ID2][exe_id].append(_task)
        #init = 0


        temp_array = np.append(temp_array,Task_exe_time)
        task_detail[stage_ID2]['num_of_task'] = task_detail[stage_ID2]['num_of_task'] + 1
        task_detail[stage_ID2]['sum_of_exetime'] = task_detail[stage_ID2]['sum_of_exetime'] + Task_exe_time
        task_exetime_stage_index[temp] = np.append(task_exetime_stage_index[temp],Task_exe_time)
        #print(temp,'\n',task_exetime_stage_index[temp])
   
   


#ジョブ関連
    if match_job:
        dict = json.loads(line)
        jobID = dict['Job ID']
        if dict['Event'] == 'SparkListenerJobStart':
            job_list.insert(jobID,{'Job_ID':jobID,'job_start_time':dict['Submission Time'],'job_end_time':0})
        if dict['Event'] == 'SparkListenerJobEnd':
            job_list[jobID]['job_end_time'] = dict['Completion Time']



#APP関連            
    if match_app:
        dict = json.loads(line)
        if dict['Event'] == 'SparkListenerApplicationStart':
            applog['app_start_time'] = dict['Timestamp']
        if dict['Event'] == 'SparkListenerApplicationEnd':
            applog['app_end_time'] = dict['Timestamp']



#ステージ関連
    if match_stage:
        dict = json.loads(line)
        stage_info = dict['Stage Info']
        stage_ID = stage_info['Stage ID']
        stage_list.insert(stage_ID,{'Stage_ID':stage_ID,'startTime':stage_info['Submission Time'],'endTime':stage_info['Completion Time']})
    line = f.readline()     
               
f.close







#ステージ、app、ジョブ、タスクごとの実行ログパース結果
print("\n",'stage_list')
for node in stage_list:
    print(node)

print("\n",'applog')
print(applog)

print("\n",'job_list')
for node in job_list:
    print(node)

print("\n",'task_list')
for node in task_list:
    print(node)

print('\n------------------------------------------------------------------------')

#タスク実行時間の平均、標準偏差を求める                                                                                                                                              
for i in range(len(task_detail)):
    task_detail[i]['num_of_task'] = task_exetime_stage_index[i].size
    task_detail[i]['sum_of_exe_time'] = np.sum(task_exetime_stage_index[i])
    task_detail[i]['avg_stage'] = np.sum(task_exetime_stage_index[i]) / task_exetime_stage_index[i].size
    task_detail[i]['std_stage'] = np.std(task_exetime_stage_index[i])
print("\ntask_detail:")
for node in task_detail:
    print(node)


#task_listをいじるならここ
#for node in task_list:
#    node['time'] = node['Timestamp'] - task_list[0]['Timestamp']
#    then = node['Timestamp']
#    node['Timestamp'] = datetime.utcfromtimestamp(then)



#exe_infoをいじるならここ
#グラフ化もここでやってみる

all_stage_num = len(stage_list)
print('all_stage_num :',all_stage_num)
print('num_of_node :',num_of_node)
fig = plt.figure()

i = 0
for stage in exe_info:
    record = all_stage_num
    column = 1
    if all_stage_num > 9:
        record = all_stage_num / 3
        column = 3
    ax = fig.add_subplot(record, column, i+1 )
    #x = np.arange(0, 500, 1)
    #y = np.sin(x)
    #test1 = ax.plot(x,y,label='fdklasf')  #ラベルがつかない！なんで？？
    ax.set_title( 'stage:' + str(i) )
    #plt.xlabel('stage exe time (ms)')
    #plt.ylabel('task duration time (ms)')
    for exe in stage.items():
        exe_num = exe[0] #{exe_num:tasklist_dict}になってたはず
        x_axes_time = []
        y_axes_exe_time = []
        print('exe_num :',exe_num)
        plt.subplots_adjust(wspace=0.05, hspace=0.6)
        if exe[1] != []:
            print('initial task : ',exe[1].pop(0)) #イニシャルタスクの処理(仮)
            init = 1
            temp11 = sorted(exe[1], key=lambda x: x['Timestamp'])
            for task in temp11: #各ステージ、各タスクごとに処理
                if init == 1:
                    first_task_timestamp = task['Timestamp']
                task['time'] = task['Timestamp'] - first_task_timestamp
                init = 0
                x_axes_time.append(task['time'])
                y_axes_exe_time.append(task['Task_exe_time'])
            stage[exe_num] = temp11
        if exe_num == '0':
            color = 'r'
        elif exe_num == '1':
            color = 'b'
        elif exe_num == '2':
            color = 'g'
        else:
            color = 'k'
        
        ax.plot(x_axes_time,y_axes_exe_time,color,linestyle='dashed',marker='.',label='fdsafa') #時間ー実行時間プロット　ラベルがつかない・・・

    i = i + 1
                          
plt.show()
                                   
        







#abnormal detection
for node in task_list:
    stage_id = node['Stage_ID']
    if node['Task_exe_time'] > task_detail[stage_id]['avg_stage'] + _k * task_detail[stage_id]['std_stage']:
        node['state'] = 1
detector = np.zeros( len(task_list) )
for i in range(2,len(task_list)-3):
    sum = task_list[i-2]['state'] + task_list[i-1]['state'] + task_list[i]['state'] + task_list[i+1]['state'] + task_list[i+2]['state'] 
#print(i,' : ',sum)
    if sum > 1:
        detector[i] = 1
#task_list[i]['state'] = 1 これはダメ
    else:
        detector[i] = 0

for i in range(2,len(task_list)-3):
    task_list[i]['state'] = int(detector[i])


#task_listをノードごとに分ける <=必要なくなったよ
#executors = {}
#for node in task_list:
#    id = node['Executor_ID']
#    if node['Executor_ID'] not in executors:
#        executors[id] = []
#    executors[id].append(node)

#print('executors')
#for node in executors.items():
#    print(node)


#さらにステージごとに分ける        
    
#1.Degree of Abnormal Ratio
num_of_node = 0

print('exe_info')
i = 0
a = 0
b = 0


#計算等はここ

for stages in exe_info:
    all_abnormals = []
    all_normals = []
    temp_for_avg_node_nor = 0
    avg_node_nor = 0 #各ステージにおける正常タスク平均実行時間の全ノードの和 Σavg_node j
    avg_node_abn = 0 #各ステージにおける異常タスク平均実行時間 avg_node j'
    print('================stage',i,'=================')
    num_of_node = len(stages) #そのステージごとにタスクを割り当てられたノードをカウントしているがどうなのだろうか
    print('num_of_node : ',num_of_node)
    for executors in stages.items():
        temp_for_avg_node_nor = 0
        abnormals = []
        normals = []
        print('=================exe',executors[0],'===================')
        for task in executors[1]:
            print(task)
            if task['state'] == 0:
                normals.append(task)
                all_normals.append(task)
            else:
                abnormals.append(task)
                all_abnormals.append(task)

        # for 2.DAD
        if normals != []:
            for nm_node in normals:
                temp_for_avg_node_nor = avg_node_nor + nm_node['Task_exe_time']
            avg_node_nor = avg_node_nor + temp_for_avg_node_nor / (len(normals))
        if abnormals != []:
            for ab_node in abnormals:
                avg_node_abn = avg_node_abn + ab_node['Task_exe_time']
        

        #1.DAR
        if all_normals != [] and all_abnormals != []:
            a = (num_of_node - 1) * len(all_abnormals) / ( len(all_normals) - len(all_abnormals) )
        #2.DAD
        if all_abnormals != []:
            avg_node_abn = avg_node_abn / len(all_abnormals)
            print('ノード数:',num_of_node)
            print('異常タスク平均実行時間:',avg_node_abn)
            print('正常タスク平均実行時間の全ノードの和:',avg_node_nor)
            b = (num_of_node - 1) * avg_node_abn / (avg_node_nor - avg_node_abn)
        
    print('a : ',a)
    print('b : ',b)
    i = i + 1


#2.Degree of Abnormal Duration












