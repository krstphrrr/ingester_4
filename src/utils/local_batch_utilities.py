import os, os.path
import pandas as pd
import logging
from datetime import datetime

def dimaDir():
  p = os.path.normcase(os.getcwd())
  return os.path.join(p,"dimas")

def extractedList(dimapath):
  lst = [dir for dir in os.listdir(dimapath) if os.path.isdir(os.path.join(dimapath,dir))]
  return lst
# dimas\extracted_NDOWVHAPublicLandsDIMA5.6asof2022.09.06_EditedCoords\NDOWVHAPublicLandsDIMA5.6asof2022.09.06_EditedCoords-tblBSNE_Box.csv

def openExtracted(extractedpath, tablename):
  p = dimaDir()
  p = os.path.join(p,extractedpath) if '/' not in extractedpath else extractedpath
  file_name = os.path.basename(p).replace('extracted_','')
  file_name = f"{file_name}-{tablename}.csv"
  file_path = os.path.join(p,file_name)
  if os.path.isfile(file_path):
    return pd.read_csv(file_path)
  else:
    logging.info("file does not exist")