from psycopg2 import connect, sql
import pandas as pd
import os, os.path
from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool
# import jaydebeapi
# import platform
# import pyodbc
import platform
import warnings
import logging

class arcno():
  __maintablelist = [
      'tblPlots',
      'tblLines',
      'tblLPIDetail',
      'tblLPIHeader',
      'tblGapDetail',
      'tblGapHeader',
      'tblQualHeader',
      'tblQualDetail',
      'tblSoilStabHeader',
      'tblSoilStabDetail',
      'tblSoilPitHorizons',
      'tblSoilPits',
      'tblSpecRichHeader',
      'tblSpecRichDetail',
      'tblPlantProdHeader',
      'tblPlantProdDetail',
      'tblPlotNotes',
      'tblPlantDenHeader',
      'tblPlantDenDetail',
      'tblSpecies',
      'tblSpeciesGeneric',
      'tblSites',
      'tblBSNE_Box',
      'tblBSNE_BoxCollection',
      'tblBSNE_Stack',
      'tblBSNE_TrapCollection',
      # # new tables
      'tblCompactDetail',
      'tblCompactHeader',
      'tblDKDetail',
      'tblDKHeader',
      'tblDryWtCompYield',
      'tblDryWtDetail',
      'tblDryWtHeader',
      'tblESDDominantPerennialHeights',
      'tblESDRockFragments',
      'tblESDWaypoints',
      'tblInfiltrationDetail',
      'tblInfiltrationHeader',
      'tblLICDetail',
      'tblLICHeader',
      'tblLICSpecies',
      'tblNestedFreqDetail',
      'tblNestedFreqHeader',
      'tblNestedFreqSpeciesDetail',
      'tblNestedFreqSpeciesSummary',
      'tblOcularCovDetail',
      'tblOcularCovHeader',
      'tblPlantDenQuads',
      'tblPlantDenSpecies',
      'tblPlantLenDetail',
      'tblPlantLenHeader',
      'tblPlotHistory',
      'tblPTFrameDetail',
      'tblPTFrameHeader',
      'tblQualDetail',
      'tblQualHeader',
      'tblSpeciesGrowthHabits',
      'tblSpeciesRichAbundance',
      'tblTreeDenDetail',
      'tblTreeDenHeader'
      ]
  correct = {
        'TBLPLOTS':'tblPlots',
        'TBLLINES':'tblLines',
        'TBLLPIDETAIL':'tblLPIDetail',
        'TBLLPIHEADER':'tblLPIHeader',
        'TBLGAPDETAIL':'tblGapDetail',
        'TBLGAPHEADER':'tblGapHeader',
        'TBLQUALHEADER':'tblQualHeader',
        'TBLQUALDETAIL':'tblQualDetail',
        'TBLSOILSTABHEADER':'tblSoilStabHeader',
        'TBLSOILSTABDETAIL':'tblSoilStabDetail',
        'TBLSOILPITHORIZONS':'tblSoilPitHorizons',
        'TBLSOILPITS':'tblSoilPits',
        'TBLSPECRICHHEADER':'tblSpecRichHeader',
        'TBLSPECRICHDETAIL':'tblSpecRichDetail',
        'TBLPLANTPRODHEADER':'tblPlantProdHeader',
        'TBLPLANTPRODDETAIL':'tblPlantProdDetail',
        'TBLPLOTNOTES':'tblPlotNotes',
        'TBLPLANTDENHEADER':'tblPlantDenHeader',
        'TBLPLANTDENDETAIL':'tblPlantDenDetail',

        'TBLSPECIES':'tblSpecies',
        'TBLSPECIESGENERIC':'tblSpeciesGeneric',
        'TBLSITES':'tblSites',
        'TBLBSNE_BOX':'tblBSNE_Box',
        'TBLBSNE_BOXCOLLECTION':'tblBSNE_BoxCollection',
        'TBLBSNE_STACK':'tblBSNE_Stack',
        'TBLBSNE_TRAPCOLLECTION':'tblBSNE_TrapCollection',
        'TBLCOMPACTDETAIL':	'tblCompactDetail',
        'TBLCOMPACTHEADER':	 'tblCompactHeader',
        'TBLDKDETAIL': 'tblDKDetail',
        'TBLDKHEADER': 'tblDKHeader',
        'TBLDRYWTCOMPYIELD': 'tblDryWtCompYield',
        'TBLDRYWTDETAIL': 'tblDryWtDetail',
        'TBLDRYWTHEADER': 'tblDryWtHeader',
        'TBLESDDOMINANTPERENNIALHEIGHTS': 'tblESDDominantPerennialHeights',
        'TBLESDROCKFRAGMENTS': 'tblESDRockFragments',
        'TBLESDWAYPOINTS': 'tblESDWaypoints',
        'TBLINFILTRATIONDETAIL': 'tblInfiltrationDetail',
        'TBLINFILTRATIONHEADER': 'tblInfiltrationHeader',
        'TBLLICDETAIL':	'tblLICDetail',
        'TBLLICHEADER':	'tblLICHeader',
        'TBLLICSPECIES': 'tblLICSpecies',
        'TBLNESTEDFREQDETAIL': 'tblNestedFreqDetail',
        'TBLNESTEDFREQHEADER':	'tblNestedFreqHeader',
        'TBLNESTEDFREQSPECIESDETAIL': 'tblNestedFreqSpeciesDetail',
        'TBLNESTEDFREQSPECIESSUMMARY': 'tblNestedFreqSpeciesSummary',
        'TBLOCULARCOVDETAIL': 'tblOcularCovDetail',
        'TBLOCULARCOVHEADER': 'tblOcularCovHeader',
        'TBLPLANTDENQUADS': 'tblPlantDenQuads',
        'TBLPLANTDENSPECIES': 'tblPlantDenSpecies',
        'TBLPLANTLENDETAIL': 'tblPlantLenDetail',
        'TBLPLANTLENHEADER': 'tblPlantLenHeader',
        'TBLPLOTHISTORY': 'tblPlotHistory',
        'TBLPTFRAMEDETAIL': 'tblPTFrameDetail',
        'TBLPTFRAMEHEADER': 'tblPTFrameHeader',
        'TBLQUALDETAIL': 'tblQualDetail',
        'TBLQUALHEADER': 'tblQualHeader',
        'TBLSPECIESGROWTHHABITS': 'tblSpeciesGrowthHabits',
        'TBLSPECIESRICHABUNDANCE': 'tblSpeciesRichAbundance',
        'TBLTREEDENDETAIL': 'tblTreeDenDetail',
        'TBLTREEDENHEADER': 'tblTreeDenHeader'
        }
  __newtables = [
       'tblHorizontalFlux',
       'tblHorizontalFlux_Locations',
       'tblDustDeposition',
       'tblDustDeposition_Locations'
      ]
  
  def __init__(self, whichdima = None, all=False):
        """ Initializes a list of tables in dima accessible on tablelist.
        ex.
        arc = arcno(path_to_dima)
        arc.tablelist
        """
        
        
        self.whichdima = whichdima
        self.tablelist=[]
        print("PRE")
        if self.whichdima is not None:
          # directory manager function provides:
          # - dima containing folder
          # - extracted directory from a dima
          # - all files from a directory with raw /trimmed names

          # self.tablelist = [self.correct[i] for i in trick if i in self.correct.keys()]
        # else:
          dpath = os.path.normpath(os.path.join(os.getcwd(),"dimas"))
          dimas = [i for i in os.listdir(dpath) if os.path.splitext(i)[1]=='.mdb']
          for i in dimas:
              dimaname = os.path.splitext(i)[0].replace(" ","")
              self.nodimapaths[dimaname] = os.path.join(dpath, dimaname)
              self.nodimadict[dimaname] = [i for i in os.listdir(os.path.join(dpath, dimaname))]
          print(self.nodimadict)
            
           


        self.actual_list = {}
        for i in self.tablelist:
            if all!=True:
                # print('false')
                if (self.MakeTableView(i,whichdima).shape[0]>=1) and (i in self.__maintablelist):
                    self.actual_list.update({i:f'rows: {self.MakeTableView(i,whichdima).shape[0]}'})
            else:
                # print('true')
                if self.MakeTableView(i,whichdima).shape[0]>=1:
                    self.actual_list.update({i:f'rows: {self.MakeTableView(i,whichdima).shape[0]}'})
