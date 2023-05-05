import json

from pretreatment_wiki import *

if __name__ == "__main__":
    # sql_manager = Data2Mysql()
    # sql_manager.label2mysql()
    line2 = get_line_iter("F:\\wiki\\latest-all.json.bz2")
    for l1 in line2:
        s = l1.strip(" ").strip("\n").strip(",")
        if s == "[" or s == "]":
            continue
        a = json.loads(l1.strip(" ").strip("\n").strip(","))
        if a['type'] != 'item':
            print(a['type'], a['id'])
