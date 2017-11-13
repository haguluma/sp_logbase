import re
import sys

argvs = sys.argv
argc = len(argvs)

#text = argvs[1]

def line_parse(str):
    if (argc != 2):
        print('Usage: # python %s text' % argvs[0])
        quit()

    text = str

    pattern1 = r"\[Times: user=([.\d]+) sys=([.\d]+), real=([.\d]+) secs\]" #実行時間等を捉える[Times: user=0.04 sys=0.01, real=0.01 secs]
    matchOB1 = re.findall(pattern1,text)

    cpu_related={'usertime':matchOB1[0][0],'systime':matchOB1[0][1],'realtime':matchOB1[0][2]}

    text2 = re.sub(pattern1,'',text) #pattern1を除く


    
    memory_related = {'timestamp':0,'state':0,'PSYoungGen':0,'before_young_obj':0,'after_young_obj':0,'young_heap_space':0,'ParOldGen':0,'before_old_obj':0,'after_old_obj':0,'old_heap_space':0,'before_obj':0,'after_obj':0,'heap_space':0,'minor_GC_exe_time':0,'GCtime':0}

    t_ptn = r"^([.\d]+):" #タイムスタンプを捉える 複数をグルーピングしていると要素が()でくくられるので注意 t_ob :  ['22.133'] | matchOB :  [('PSYoungGen', '11586', '0', '332288')]
    t_ob = re.findall(t_ptn,text2)
#    print('t_ob : ',t_ob)
#    if t_ob != []:

    memory_related['timestamp'] = t_ob[0]

    pattern2 =r"\[[^\[\]]+\]" #ガベージコレクションの内容[PSYoungGen: 159651K->10388K(313344K)]などを捉える 
    matchOB2 = re.findall(pattern2,text2)
#    print('matchOB2',matchOB2)
    for mynode in matchOB2:
        mypattern = r"\[(\w+): (\d+)K->(\d+)K\((\d+)K\)]"
        matchOB = re.findall(mypattern,mynode)
        #print(mynode,'\n')
        #print('matchOB : ',matchOB)
        if mynode != []:
            if matchOB[0][0] == "PSYoungGen":
                memory_related['before_young_obj'] = matchOB[0][1]
                memory_related['after_young_obj'] = matchOB[0][2]
                memory_related['young_heap_space'] = matchOB[0][3]
                memory_related['PSYoungGen'] = 1
            if matchOB[0][0] == "ParOldGen":
                memory_related['before_old_obj'] = matchOB[0][1]
                memory_related['after_old_obj'] = matchOB[0][2]
                memory_related['old_heap_space'] = matchOB[0][3]
                memory_related['ParOldGen'] = 1
            #if matchOB[0] == "Metaspace":
                #memory_related[''] = matchOB[1]
                #memory_related[''] = matchOB[2]
                #memory_related[''] = matchOB[3]



    text3 = re.sub(pattern2,'',text2) #patter2を除く

    pattern3 = r"([ \w]+ \([ \w]+\)) +(\d+)K->(\d+)K\((\d+)K\)[ ,]+([.\d]+) secs]"
    matchOB3 = re.findall(pattern3,text3)
    if matchOB3 != []:
        memory_related['state'] = matchOB3[0][0]
        memory_related['before_obj'] = matchOB3[0][1]
        memory_related['after_obj'] = matchOB3[0][2]
        memory_related['heap_space'] = matchOB3[0][3]
        memory_related['GCtime'] = matchOB3[0][4]
    
    #print('memory_related',memory_related)
                
    return [cpu_related,memory_related]

#parse_result = line_parse(text)

#print('cpu_related->\n',parse_result[0])
#print('memory_related->\n',parse_result[1])


#print('The content of %s ...n' % argvs[1])
#f = open(argvs[1])
#line = f.readline()
#while line:
#    print(line)
#    line = f.readline()
#f.close


