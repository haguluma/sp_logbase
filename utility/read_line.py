import re
import sys
import json

argvs = sys.argv
argc = len(argvs)

text = argvs[1]

if (argc != 2):
    print('Usage: # python %s text' % argvs[0])
    quit()

f = open(argvs[1])                                                                                                                                                                                                               
line = f.readline()
                                                                                                                                                                                           
while line:
    print(line)
    line = f.readline()                                                                                                                                                                                                          



f.close       
