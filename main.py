import sys
import os
import pandas as pd
import urllib.request
from time import time       
import json 

names = ['GKGRECORDID', 'DATE', 'SOURCECOLLECTIONIDENTIFIER',
         'SOURCECOMMONNAME', 'DOCUMENTIDENTIFIER', 'COUNTS', 'COUNTS_2',
         'THEMES', 'ENHANCEDTHEMES', 'LOCATIONS', 'ENHANCEDLOCATIONS',
         'PERSONS', 'ENHANCEDPERSONS', 'ORGANIZATIONS',
         'ENHANCEDORGANIZATIONS', '5TONE', 'ENHANCEDDATES', 'GCAM',
         'SHARINGIMAGE', 'RELATEDIMAGES', 'SOCIALIMAGEEMBEDS',
         'SOCIALVIDEOEMBEDS', 'QUOTATIONS', 'ALLNAMES', 'AMOUNTS',
         'TRANSLATIONINFO', 'EXTRASXML']

sel_names = ['GKGRECORDID', 'DATE', 'SOURCECOMMONNAME', 'DOCUMENTIDENTIFIER',
             'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', '5TONE']

def download_list(prefix, out):
    df_files = pd.read_csv(f'{out}temp/{prefix}/masterfilelist.txt', header=None, sep=' ')
    df_files[2] = df_files[2].fillna('')
    df_files = df_files.loc[df_files[2].apply(lambda x: 'gkg' in x)]

    files = pd.Series([file for file in df_files[2] if prefix in file])
    # files = files.head(2)
    
    for file in files:
        out_file = file.replace('http://data.gdeltproject.org/gdeltv2/', '')
        if not os.path.exists(f"{out}temp/{prefix}/{out_file}"):
            print(f'Downloading {file}\r', end='')
            urllib.request.urlretrieve(file, 
                                       f"{out}temp/{prefix}/{out_file}")
    
def filter_datasets(prefix, out):
    out_file = f'{out}csv/{prefix}_filtered.csv'
    
    if os.path.exists(out_file):
        return {}
    
    themes = set(pd.read_csv('./agro_themes.csv', header=None).apply(lambda x: x[0], axis=1).values)
    original_no_articles = 0
    filtered_no_articles = 0
    with open(out_file, 'w') as o:
        for file in os.listdir(f'{out}temp/{prefix}'):
            try:
                if not file.endswith(".zip"):
                    continue
                print(f'Filtering {file}\r', end='')
                df = pd.read_csv(f'{out}temp/{prefix}/{file}', sep='\t', names=names, usecols=sel_names, encoding="ISO-8859-1")
                
                original_no_articles += df.shape[0]
                df = df.loc[df['THEMES'].fillna('').apply(lambda x: len(set(x.split(';')) & themes) > 0)]
                filtered_no_articles += df.shape[0]
                df.to_csv(o, header=None, index=None)
            except:
                continue
            
    return {'original_no_articles': original_no_articles, 
            'filtered_no_articles': filtered_no_articles}

    
def make_dataset(prefix, out):
    csv_file = f'{out}csv/{prefix}_filtered.csv'
    
    if not os.path.exists(csv_file):
        print(f'Creating {csv_file}')
        if not os.path.exists(f'{out}temp/{prefix}'):
            os.mkdir(f'{out}temp/{prefix}')
            
        if not os.path.exists(f'{out}temp/{prefix}/masterfilelist.txt'):    
            urllib.request.urlretrieve("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", 
                                   f"{out}temp/{prefix}/masterfilelist.txt")   
            
        download_list(prefix, out)
        return filter_datasets(prefix, out)
        # os.rmdir(f'{out}temp/{prefix}')    

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
