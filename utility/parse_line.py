import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/')

import re
import sys
import json
import dict_dump

argvs = sys.argv
argc = len(argvs)

text = argvs[1]

if (argc != 2):
    print('Usage: # python %s text' % argvs[0])
    quit()

f = open(argvs[1])                                                                                                                                                                                                               
line = f.readline()
                                                                                                                                                                                           
while line:
    dict = json.loads(line)
    format_json = json.dumps(dict, indent=4, separators=(',', ': '))
    print(format_json)
    line = f.readline()                                                                                                                                                                                                          

f.close       
