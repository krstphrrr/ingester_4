import os, os.path
import pandas as pd
import logging
import json
import re
from datetime import datetime

def localLooper( tablename, path2mdbs=None, projk=None, pk_formdate_range=None):
  # dimadir - could have multiples directories each with files
  containing_folder = path2mdbs if path2mdbs is not None else dimaDir()

  contained_dirs = extractedList(containing_folder)
  df_dictionary = {}

  count = 1
  basestring = 'file_'
  projk = "placeholder" if projk is None else projk
  pk_formdate_range = 0 if pk_formdate_range is None else pk_formdate_range

  for dir in contained_dirs:
    for file in dir:
      countup = basestring+str(count)
      tbl = ""

def form_date_check(extractedpath):
  obj = dict()
  for i in os.listdir(extractedpath):
    df = pd.read_csv(os.path.join(extractedpath,i),engine='python-fwf', nrows=0)
    tbl_columns = [col for col in df.columns]
    pattern = re.search('$(tbl.*?$).csv',i, re.DOTALL)

    if 'FormDate' in tbl_columns:
        obj[pattern[0]] = True
    else:
        obj[pattern[0]] = False
  return obj

config = r"/usr/src/utils/configs.json"
class TableHandler:

    def __init__(self, extractedpath, pk_formdate_range, tablename):
        # self.bsne = json.load(open(file=config))["bsne"]
        self._join_key = json.load(open(file=config))["joinkeys"][tablename]
        self._extractedpath = extractedpath
        self._table_name = tablename
        self._bsne_check = any([
            i for i in form_date_check(self._dimapath).keys() if "tblBSNE" in i
            ])

        self.raw_table = openExtracted(self._extractedpath, self._table_name)

        self.table_pk = self.get_pk(pk_formdate_range)

    def get_pk(self, custom_daterange):

        # if self._table_name in self.bsne and self._bsne_check==True:
        #     print("bsne")
        #     self.pk_source = pk_appender_bsne(self._dimapath,
        #         custom_daterange).drop_duplicates(ignore_index=True)
        # elif self._table_name in self.bsne and self._bsne_check==False:
        #     print("not bsne")
        #     self.pk_source = pk_appender(self._dimapath,
        #         custom_daterange).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            return pd.merge(
                self.raw_table,
                self.pk_source.filter([self._join_key,
                                       'PrimaryKey'
                                       ]).drop_duplicates(ignore_index=True),
                how="inner", on=self._join_key).loc[:,cols]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])



def dimaDir():
  p = os.path.normcase(os.getcwd())
  return os.path.join(p,"dimas")

def extractedList(dimapath):
  lst = [dir for dir in os.listdir(dimapath) if os.path.isdir(os.path.join(dimapath,dir))]
  return lst
# dimas\extracted_NDOWVHAPublicLandsDIMA5.6asof2022.09.06_EditedCoords\NDOWVHAPublicLandsDIMA5.6asof2022.09.06_EditedCoords-tblBSNE_Box.csv

def openExtracted(extractedpath, tablename):
  # extractedpath: /usr/src/dimas/extracted_****
  p = dimaDir()
  p = os.path.join(p,extractedpath) if '/' not in extractedpath else extractedpath
  file_name = os.path.basename(p).replace('extracted_','')
  file_name = f"{file_name}-{tablename}.csv"
  file_path = os.path.join(p,file_name)
  if os.path.isfile(file_path):
    return pd.read_csv(file_path, engine='python-fwf')
  else:
    print("file does not exist")
    # logging.info("file does not exist")

def header_detail(formdate_dictionary, extractedpath):
  pattern_to_remove = re.compile(r'(tbl)|(Header.csv)')
  headers =[i for i in os.listdir(extractedpath) if 'eader' in i]
  details = [i for i in os.listdir(extractedpath) if 'etail' in i]
  print(formdate_dictionary)
  print(headers)
  print(details)
  skip_table = []
  for key in formdate_dictionary.keys():
      header_keys = [i for i in headers if key in i]
      detail_keys = [i for i in details if key in i]
      print("HEADER KEYS",header_keys)
      print("DETAIL KEYS", detail_keys)
      if formdate_dictionary[key]==True:
          print(key)
          simple_table_name = pattern_to_remove.sub('',key)
          print("SIMPLETABLENAME: ",simple_table_name)
          header = openExtracted(extractedpath, f'tbl{simple_table_name}Header')
          detail = openExtracted(extractedpath, f'tbl{simple_table_name}Detail')
          # this conditional handles dimas with missing details table
          if detail.shape[0]>1:
              df = pd.merge(header,detail, how="outer", on="RecKey")
              # df = header
              return df
          else:
              return header