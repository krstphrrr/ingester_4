from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import horizontalflux_raw, \
pk_appender_bsne
import os
import pandas as pd
import logging

class HorizontalFlux:
    _table_name = "tblHorizontalFlux"
    _join_key = "BoxID"

    def __init__(self, dimapath, pk_formdate_range):
        self._dimapath = dimapath
        logging.info(f"extracting the {self._table_name} from the {os.path.basename(self._dimapath).replace(' ','')} dimafile..")
        self.raw_table = horizontalflux_raw(self._dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")

        self.table_pk = self.get_pk(pk_formdate_range)
        logging.info("PrimaryKey added.")
        self.final_df = self.tbl_fixes(self.table_pk)

    def get_pk(self, custom_daterange):
        # primary key flow
        self.pk_source = pk_appender_bsne(
            self._dimapath,
            custom_daterange).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            # return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
            return pd.merge(
                self.raw_table,
                self.pk_source,
                suffixes=(None, '_y'),
                how="left",
                left_on=["BoxID","StackID","collectDate"],
                right_on=["BoxID","StackID","collectDate"]
            )[cols].drop_duplicates(["BoxID","StackID","RecKey"], ignore_index=True)
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])

    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        if 'ovenTemp' in df.columns:
            df.ovenTemp = df.ovenTemp.apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        return df
