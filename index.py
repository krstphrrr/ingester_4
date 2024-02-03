import os, os.path, sys
from cmd import Cmd
import logging
from src.project.project import all_dimas, update_project
from src.utils.batch_utilities import batch_looper, table_collector
from src.utils.report import csv_report
from src.tables.tbllines import Lines
from src.utils.batch_utilities import table_operations, looper
from src.utils.new_export import subTest

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)


class main(Cmd):

    # constructor: pull up what's available and make it a list/dict
    # do_extract: run the bash script inside container
    # do_ingest: run ingester. 
    def __init__(self):
        super(main, self).__init__()
        self.prompt = "> "
        self.batch_path = os.path.normpath(os.path.join(os.getcwd(),"dimas"))
        self.dimafiles = [os.path.normpath(f"{self.batch_path}/{i}") for i in os.listdir(self.batch_path)]
        # create a list of directories 
        self.dimacsvs = [os.path.normpath(f"{self.batch_path}/{i}") for i in os.listdir(self.batch_path) if "extracted_" in i]

    
    def do_printcsvs(self, args):
        # check constructor list of extracted directories
        self.dimacsvs = [os.path.normpath(f"{self.batch_path}/{i}") for i in os.listdir(self.batch_path) if "extracted_" in i]
        print(self.dimacsvs)
    
    def do_extract(self, args):
        """ extrat tables from mdb using mdbtools bash script, 
        deposits them in directory with same name as mdb file + 'extracted_'
        """
        subTest()
        
    def do_ingest(self,args):

        path_to_csvs = args
        print(path_to_csvs)
        # modified batch looper (collects a single tables across all dimas)

        # arguments
        projectkey, pk_formdate_range, dev_or_not = args.split(" ", 3)
        dev_obj = {
            'True':"dimadev",
            True:"dimadev",
            'False':"dima",
            False:"dima"
            }
        # check for project file
        if any([True for i in os.listdir(self.batch_path) if '.xlsx' in os.path.splitext(i)[1]]):
            print(dev_or_not,"inside index, prebatch_looper")
            logging.info(f"inside index, prebatch_looper: {dev_or_not}")
            # batch looper handles multiple extracted dimas 
            batch_looper(self.batch_path, projectkey, dev_or_not, pk_formdate_range)
            update_project(self.batch_path, projectkey, dev_obj[dev_or_not])
        else:
            print("No project file found within dima directory; unable to ingest.")

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: printcsvs, extract, ingest, exit, help.")
