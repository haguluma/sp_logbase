import re
import sys
import json

def dict_tree(dict):
    f = open("./output.json", "a")
    json.dump(dict, f, ensure_ascii=False, indent=4, sort_keys=True, separators=(',', ': '))
