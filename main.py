import sys
import os
import pandas as pd
import urllib.request
from time import time       
import json 
from utils import make_dataset


if __name__ == '__main__':
    print(sys.argv)
    # if len(sys.argv) != 4:
    #     raise ValueError('Please give date and directory!')
    prefix = sys.argv[1]
    out = sys.argv[2]
    log_file = sys.argv[3]
    
    t1 = time()
    log = make_dataset(prefix, out)
    t2 = time()
    
    log['time'] =  t2-t1
    
    with open(log_file, 'w') as o:
        o.write(json.dumps(log))
