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
    # dfs['date'] = []

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
            # if (modified_i not in dfs['formdate']) or (modified_i not in dfs['collectdate']) or (modified_i not in dfs['daterecorded']):
    return dfs

def usable_formdate_table_finder(datefield_finder_dict):
    usable_tables = []
    for i in datefield_finder_dict['formdate']:
        if i[1][0] > 0:
            usable_tables.append(i[0])
    return usable_tables

def header_detail(path, usable_table):
    lst = [i for i in os.listdir(path)]
    header = None
    detail = None
    det_path = None
    which = usable_table

    for i in lst:
        if which in i and "Header" in i:

            header = pd.read_csv(os.path.join(path, i), engine='python')
            det = i.replace("Header","Detail")
            det_path = os.path.join(path, det)
    detail = pd.read_csv(det_path, engine='python')

    header_det = pd.merge(header,detail, how="inner", on="RecKey")
    return header_det

def new_form_date(old_formdate_dataframe):
    try:
        if "FormDate" in old_formdate_dataframe.columns:
            old_formdate_dataframe = old_formdate_dataframe[~pd.isna(old_formdate_dataframe.FormDate)].copy()
            which_field = 'FormDatePK'
            which_field_original = 'FormDate'
        elif "collectDate" in old_formdate_dataframe.columns:
            old_formdate_dataframe = old_formdate_dataframe[~pd.isna(old_formdate_dataframe.collectDate)].copy()
            which_field = 'collectDatePK'
            which_field_original = 'collectDate'
        elif "DateRecorded" in old_formdate_dataframe.columns:
            old_formdate_dataframe = old_formdate_dataframe[~pd.isna(old_formdate_dataframe.DateRecorded)].copy()
            which_field = "DateRecordedPK"
            which_field_original = "DateRecorded"
    except Exception as e:
        print(e, "no usable daterange")
    finally:
        old_formdate_dataframe[which_field] = old_formdate_dataframe[which_field_original]
        return old_formdate_dataframe

def get_plotkeys(path):
    # getting correct prefix of file inside dima path (remove extracted_)
    basename= os.path.basename(path).replace("extracted_","")
    # create a correct path to the extracted dima directory
    basepath = os.path.join(os.getcwd(), os.path.normpath(path))

    # create correct filenames for plot and line csv files
    linefile = basename + "-tblLines.csv"
    plotfile = basename + "-tblPlots.csv"

    # create dataframes with: correct path + correct filename
    lines = pd.read_csv(os.path.join(basepath, linefile))
    plots = pd.read_csv(os.path.join(basepath, plotfile))

    # merge line +plot
    line_plot = pd.merge(lines, plots, how="inner", on="PlotKey")
    return line_plot

def pk_appender(path, tablename):
    # object with usable datefields and the table that has them
    all_formdate_tables = datefield_finder(path)
    # create a header+detail dataframe using
    usable_tables= usable_formdate_table_finder(all_formdate_tables)

    header_det_df = header_detail(path, usable_tables[0])
    new_formdate_df = new_form_date(header_det_df)

    line_plot = get_plotkeys(path)

    if ("SoilStab" in tablename) or ("DK" in tablename) or ("Infiltration" in tablename) or ("TreeDen" in tablename):
        full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="PlotKey").drop_duplicates(["PlotKey", "RecKey"], ignore_index=True)
    elif "Plots" in tablename:
        if "LineKey" in new_formdate_df.columns and "LineKey" in line_plot.columns:
            full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="LineKey")
        else:
            full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="PlotKey").drop_duplicates(["PlotKey", "RecKey"],ignore_index=True)
    else:
        full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="LineKey").drop_duplicates(["LineKey", "RecKey"], ignore_index=True)
    # return full_join
    if 'PlotKey_x' in full_join.columns:
        full_join.drop(['PlotKey_x'], axis=1, inplace=True)
        full_join.rename(columns={'PlotKey_y':"PlotKey"}, inplace=True)
        full_join = full_join.drop_duplicates(["PlotKey", "RecKey"],ignore_index=True)
    full_join["FormDatePK"] = full_join.FormDatePK.apply(lambda x: pd.to_datetime(x).date())
    final_df = column_join(full_join,"PrimaryKey", "PlotKey", "FormDatePK")
    return final_df


def column_join(in_df,newfield,*fields):
    in_df[f'{newfield}'] = (in_df[[f'{field}' for field in fields]].astype(str)).sum(axis=1)
    return in_df

def make_table_view(path, tablename):
    # getting correct prefix of file inside dima path (remove extracted_)
    basename= os.path.basename(path).replace("extracted_","")
    # create a correct path to the extracted dima directory
    basepath = os.path.join(os.getcwd(), os.path.normpath(path))

    # create correct filenames for table files
    tablefile = basename + f"-{tablename}.csv"

    # create dataframes with: correct path + correct filename
    table = pd.read_csv(os.path.join(basepath, tablefile))
    return table
