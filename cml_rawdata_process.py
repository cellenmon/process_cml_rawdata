import pandas as pd
import numpy as np
import pyproj
from pathlib import Path
from os import listdir
from os.path import isfile, join, exists
import matplotlib.pyplot as plt
import pickle as pkl

class CmlRawdataProcessor:
    def __init__(self, raw_data_path, metadata_path, sel_links_path=None):
        self.raw_data_path = raw_data_path
        self.metadata_path = metadata_path
        self.sel_links_path = sel_links_path

        for i in range(1000):
            temp_str = 'output_' + str(i)
            out_path = Path.joinpath(Path.cwd(),temp_str)
            if not Path(out_path).is_dir():
                Path.mkdir(out_path)
                self.out_path = out_path
                break
            if i==999:
                raise Exception("You seem to have too many output directories...")

    def cellcom_ids(self, site_id):
        ''' remove IP numbers from cellcom site_a_id/site_b_id,
        and also convert all letters to lower case'''
        if type(site_id) == float:
            return np.nan
        elif len(site_id.strip('.0123456789;')) == 0:
            return np.nan
        elif '; ' in site_id:
            return site_id.split('; ')[1]
        else:
            return site_id[0:4]

    def process_cellcom(self, xlfile, col_names):
        ''' Process metadata for cellcom '''
        #     xl = pd.ExcelFile(xlfile)

        #     df = xl.parse('Sheet1', skiprows=1) # skip the first row of the excel file
        if '.xls' in xlfile:
            df = pd.read_excel(xlfile)
        else:
            df = pd.read_csv(xlfile)
        cols = ['LINK_NO', 'STATUS', 'TX_FREQ_HIGH_MHZ', 'TX_FREQ_LOW_MHZ', 'POL',
                'LENGTH_KM', 'SITE1_NAME', 'ID_SITE1', 'EAST1', 'NORTH1',
                'HEIGHT_ABOVE_SEA1_M', 'SITE2_NAME', 'ID_SITE2', 'EAST2',
                'NORTH2', 'HEIGHT_ABOVE_SEA2_M']
        df = df[cols]
        df['link_id'] = df['ID_SITE1'] + '-' + df['ID_SITE2']
        df['link_id'] = df['link_id'].str.lower()
        df.insert(16, 'SLOTS', '')
        df.insert(0, 'SP', 'cellcom')
        df.columns = col_names

        # convert EAST/NORTH to LAT/LON decimal
        bng = pyproj.Proj(init='epsg:2039')
        wgs84 = pyproj.Proj(init='epsg:4326')
        # lon, lat = pyproj.transform(from,to,easting,northing)
        df['LON1'], df['LAT1'] = pyproj.transform(bng, wgs84, df['LON1'].values, df['LAT1'].values)
        df['LON2'], df['LAT2'] = pyproj.transform(bng, wgs84, df['LON2'].values, df['LAT2'].values)

        # process cellcom ids to fix problems
        df['SITE1_ID'] = df['SITE1_ID'].apply(self.cellcom_ids)
        df['SITE2_ID'] = df['SITE2_ID'].apply(self.cellcom_ids)

        # remove '-X' from cml_id
        df['Link_num'] = df['Link_num'].str.partition('-')[0]
        return df

    def execute(self):
        self.metadata_path
        # process all the metadata
        col_names = ['SP', 'Link_num', 'Status', 'Frequency1',
                     'Frequency2', 'Polarization', 'Length_KM',
                     'SITE1_Name', 'SITE1_ID', 'LON1', 'LAT1',
                     'Height_above_sea1', 'SITE2_Name', 'SITE2_ID',
                     'LON2', 'LAT2', 'Height_above_sea2', 'SLOTS', 'link_id']
        MD = self.process_cellcom(str(self.metadata_path), col_names)

        # convert object to numeric values with additional processing
        # MD.loc[:,'Link_num'] = pd.to_string(MD.loc[:,'Link_num'], errors='coerce')

        # convert MHz to GHz
        MD.loc[:, 'Frequency1'] = pd.to_numeric(MD.loc[:, 'Frequency1'], errors='coerce') * 1e9 / 1000
        MD.loc[:, 'Frequency2'] = pd.to_numeric(MD.loc[:, 'Frequency2'], errors='coerce') * 1e9 / 1000

        MD.loc[:, 'LAT1'] = pd.to_numeric(MD.loc[:, 'LAT1'], errors='coerce')
        MD.loc[:, 'LON1'] = pd.to_numeric(MD.loc[:, 'LON1'], errors='coerce')
        MD.loc[:, 'LAT2'] = pd.to_numeric(MD.loc[:, 'LAT2'], errors='coerce')
        MD.loc[:, 'LON2'] = pd.to_numeric(MD.loc[:, 'LON2'], errors='coerce')
        MD.loc[:, 'Length_KM'] = pd.to_numeric(MD.loc[:, 'Length_KM'], errors='coerce')

        MD.loc[:, 'Height_above_sea1'] = pd.to_numeric(MD.loc[:, 'Height_above_sea1'], errors='coerce')
        MD.loc[:, 'Height_above_sea2'] = pd.to_numeric(MD.loc[:, 'Height_above_sea2'], errors='coerce')

        # select raw-data files to open
        only_files = sorted([f for f in listdir(self.raw_data_path) if '.txt' in f])

        if self.sel_links_path:
            print('Filtering selected links')
            sel_links = list(np.genfromtxt(str(sel_links_path), dtype='str'))
            sites = []
            for i, link in enumerate(sel_links):
                l = str(link).partition('-')
                sites.append(l[0].lower())
                sites.append(l[-1].lower())

        self.RD_rx = []  # gather all RADIO_SINK
        self.RD_tx = []  # gather all RADIO_SOURCE

        for rdfile in only_files[0:2]:
            rdfile = str(self.raw_data_path.joinpath(rdfile))
            RD = pd.read_csv(rdfile, index_col=False)
            RD.insert(6, 'Site', '')
            RD['Site'] = RD['NeAlias'].str.partition('_')[0]
            RD['Site'] = RD['Site'].str.lower()
            RD['NeAlias'] = RD['NeAlias'].str.rpartition('_')[2]
            RD['NeAlias'] = RD['NeAlias'].str.rpartition('.')[0]

            if self.sel_links_path:
                RD = RD.loc[RD['Site'].isin(sites)]

                # separate to RX and TX
            if str.find(rdfile, 'RADIO_SINK') != -1:
                RD = RD[['Time', 'Interval', 'Site', 'NeAlias', 'PowerRLTMmin', 'PowerRLTMmax']]
                self.RD_rx.append(RD)

            elif str.find(rdfile, 'RADIO_SOURCE') != -1:
                RD = RD[['Time', 'Interval', 'Site', 'NeAlias', 'PowerTLTMmin', 'PowerTLTMmax']]
                self.RD_tx.append(RD)

        self.RD_rx = pd.concat(self.RD_rx)  # the min/max RSL
        self.RD_tx = pd.concat(self.RD_tx)  # the min/max TSL

        # replace NeAlias with link_number
        self.RD_rx = self.RD_rx.rename(columns={'NeAlias': 'Hop_number', 'Site': 'Measuring_site'})
        self.RD_tx = self.RD_tx.rename(columns={'NeAlias': 'Hop_number', 'Site': 'Measuring_site'})

        # take only 15 minute data
        self.RD_rx = self.RD_rx[self.RD_rx['Interval'] == 15]
        self.RD_tx = self.RD_tx[self.RD_tx['Interval'] == 15]

        hops = []
        hops.append(self.RD_tx['Hop_number'].unique())
        hops = list(hops[0])
        print(hops)

        self.RD_rx['Link_number'] = '-'
        self.RD_tx['Link_number'] = '-'
        hops_to_drop = []
        for h, hop in enumerate(hops):
            rsl = self.RD_rx[self.RD_rx['Hop_number'] == hop]
            rsl_temp_sites = sorted(rsl['Measuring_site'].unique())
            tsl = self.RD_tx[self.RD_tx['Hop_number'] == hop]
            tsl_temp_sites = sorted(tsl['Measuring_site'].unique())

            if (rsl_temp_sites==tsl_temp_sites) & (len(tsl_temp_sites)==2):
                ##Rx up
                self.RD_rx['Link_number'] = np.where(
                    (self.RD_rx['Hop_number'] == hop) &
                    (self.RD_rx['Measuring_site'] == rsl_temp_sites[0]),
                    tsl_temp_sites[1] + '-' + rsl_temp_sites[0],
                    self.RD_rx['Link_number']
                )
                ##Rx down
                self.RD_rx['Link_number'] = np.where(
                    (self.RD_rx['Hop_number'] == hop) &
                    (self.RD_rx['Measuring_site'] == rsl_temp_sites[1]),
                    tsl_temp_sites[0] + '-' + rsl_temp_sites[1],
                    self.RD_rx['Link_number']
                )
                ## Tx up
                self.RD_tx['Link_number'] = np.where(
                    (self.RD_tx['Hop_number'] == hop) &
                    (self.RD_tx['Measuring_site'] == tsl_temp_sites[1]),
                    tsl_temp_sites[1] + '-' + rsl_temp_sites[0],
                    self.RD_tx['Link_number']
                )
                ## Tx down
                self.RD_tx['Link_number'] = np.where(
                    (self.RD_tx['Hop_number'] == hop) &
                    (self.RD_tx['Measuring_site'] == tsl_temp_sites[0]),
                    tsl_temp_sites[0] + '-' + rsl_temp_sites[1],
                    self.RD_tx['Link_number']
                )

        self.RD_rx.to_csv(self.out_path.joinpath('rd_rx.csv'))
        self.RD_tx.to_csv(self.out_path.joinpath('rd_tx.csv'))


if __name__ == "__main__":
    raw_data_path = Path.joinpath(Path.cwd(), 'raw')
    metadata_path = Path.joinpath(Path.cwd(), 'metadata').joinpath(
        'New_Celltable_final_converted.xls'
    )
    sel_links_path = Path(
        '/Users/adameshel/Documents/Python_scripts/process_cml_rawdata/selected_links.txt'
    )
    crp = CmlRawdataProcessor(raw_data_path,metadata_path)
    crp.execute()
    print('All outputs were generated in:')
    print(str(crp.out_path))