import pandas as pd
import os

"""
1. create pk
2. return pk to table class 
"""

def datefield_finder(csvs_path):
    dfs = dict()
    dfs['formdate'] = []
    dfs['collectdate'] = []
    dfs['daterecorded'] = []

    for i in os.listdir(csvs_path):
        """
        creates a dictionary that segretates tables on the datefields they possess
        """
        base = i.split('-')[1]
        modified_i = base.split(".")[0]
        # print(modified_i)
        df = pd.read_csv(os.path.join(csvs_path,i), engine='python', nrows=5)
        
        if "FormDate" in df.columns:
            dfs['formdate'].append([modified_i, df.shape])
            
        elif "collectDate" in df.columns:
            dfs['collectdate'].append([modified_i, df.shape])

        elif "DateRecorded" in df.columns:
            dfs['daterecorded'].append([modified_i, df.shape])
    return dfs

def formdate_correction(dfs_dict):
    """
    """
    working_tables = []
    for i in dfs_dict['formdate']:
        if i[1][0] > 0:
            working_tables.append(i[0])
    return working_tables

def header_det_assembler_tst(path, table):
    """
    """
    lst = [i for i in os.listdir(path)]
    header = None 
    detail = None
    det_path = None
    which = table

    for i in lst:
        if which in i and "Header" in i:

            header = pd.read_csv(os.path.join(path, i), engine='python')
            det = i.replace("Header","Detail")
            det_path = os.path.join(path, det)
    # if det_path exists, use det_path
    detail = pd.read_csv(det_path, engine='python')

    header_det = pd.merge(header,detail, how="outer", on="RecKey")
    return header_det