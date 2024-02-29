
from src.primarykeys.local_primarykey_functions import (
    pk_appender,
    make_table_view)
    # pk_appender_bsne,
    # form_date_check)
    # pk_appender_soil)

import json

import os
import pandas as pd
import logging

config = [os.path.join(os.getcwd(),os.path.normpath("./src/utils"),i) for i in os.listdir(os.path.join(os.getcwd(),os.path.normpath("./src/utils"))) if 'configs' in i][0]


class TableHandler:
    """
    1. primarykeys for bsne:
    - if formdate found on bsne tables join and make pk field
    - - pk  =
    2. make raw table:
    - create a table (no joins) from csv
    3. get_pk
    """

    def __init__(self, dimapath, tablename):
        # self.bsne = json.load(open(file=config))["bsne"]
        self._join_key = json.load(open(file=config))["joinkeys"][tablename]
        # self._join_key = ""
        self._dimapath = dimapath
        self._table_name = tablename
        # self._bsne_check = any([
        #     i for i in form_date_check(self._dimapath).keys() if "tblBSNE" in i
        #     ])

        self.raw_table = make_table_view(
            self._dimapath,
            self._table_name
            )

        self.table_pk = self.get_pk(self._dimapath, self._table_name)

    def get_pk(self,path, tablename):

        # if self._table_name in self.bsne and self._bsne_check==True:
        #  print("bsne")
        #     self.pk_source = pk_appender_bsne(self._dimapath,
        #         custom_daterange).drop_duplicates(ignore_index=True)
        # elif self._table_name in self.bsne and self._bsne_check==False:
        #     print("not bsne")
        self.pk_source = pk_appender(self._dimapath, tablename).drop_duplicates(ignore_index=True)

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
