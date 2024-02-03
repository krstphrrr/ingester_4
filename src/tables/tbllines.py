from src.utils.database_functions import arcno
from src.utils.utility_functions import Timer
from src.primarykeys.primary_key_functions import pk_appender,form_date_check, pk_appender_bsne
import os
import pandas as pd
import logging

class Lines:
    _table_name = "tblLines"
    _join_key = "LineKey"

    def __init__(self, dimapath, pk_formdate_range):

        self._dimapath = dimapath
        logging.info(f"extracting the {self._table_name} from the {os.path.basename(self._dimapath).replace(' ','')} dimafile..")
        self.raw_table = arcno.MakeTableView("tblLines", dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(pk_formdate_range)

        self.final_df = self.tbl_fixes(self.table_pk)

    def get_pk(self, custom_daterange):
        tables_with_formdate = form_date_check(self._dimapath)


        if any(tables_with_formdate.values())==False and any([i for i in tables_with_formdate.keys() if "tblBSNE" in i])==True:

            self.pk_source = pk_appender_bsne(
                self._dimapath,
                custom_daterange).drop_duplicates(ignore_index=True)


        else:

            self.pk_source = pk_appender(
                self._dimapath,
                custom_daterange,
                self._table_name).drop_duplicates(ignore_index=True)


        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            # return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
            linekeyfix = self.missing_linekey_fix(self.pk_source)
            missingpks=  pd.merge(
                            self.raw_table,
                            self.pk_source,
                            suffixes=(None, '_y'),
                            how="left",
                            left_on = linekeyfix['leftright'],
                            right_on = linekeyfix['leftright']
                        )[cols].drop_duplicates( linekeyfix['dropdups'],
                                                ignore_index=True)

            return missingpks[~pd.isna(missingpks.PrimaryKey)]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        if self._join_key in self.raw_table.columns:
            if ("888888888" in df[self._join_key].unique()) or ('999999999' in df[self._join_key].unique()):
                return df[(df[self._join_key] != "888888888") & (df[self._join_key] != "999999999")]
        if "Azimuth" in self.raw_table.columns:

            df.Azimuth = df.Azimuth.apply(lambda x: pd.NA if pd.isna(x) else x).astype("Int64")
        return df

    def missing_linekey_fix(self, df):
        obj = {}
        if 'PlotKey' in df.columns and 'LineKey' in df.columns:
            obj['leftright']= ['PlotKey', "LineKey"]
            obj['dropdups'] = ['LineKey']
        elif 'PlotKey' in df.columns and 'LineKey' not in df.columns:
            obj['leftright'] = ["PlotKey"]
            obj['dropdups'] = ['PlotKey']
        return obj














#
