# import our libraries
import requests
import pandas as pd
import os
import json
import time
from bs4 import BeautifulSoup
from tinydb import TinyDB, Query


# init the tinydb database to store processed filings
db = TinyDB('db.json')
processed_filings_table = db.table('processed_filings')

# define a function to clean file name. replace '/' with '-'
def clean_filename(filename):
    return filename.replace("/", "-")


# define a function that will make the process of building a url easy.
def make_url(base_url, comp):
    url = base_url

    # add each component to the base url
    for r in comp:
        url = '{}/{}'.format(url, r)

    return url


# define a function to parse an NPORT-P form type filing
def parse_nport_form(filing_url):
    doc_dict = {}

    # request the url, and then parse the response.
    response = requests.get(filing_url)
    if response.status_code == 200:

        soup = BeautifulSoup(response.content, 'xml')

        doc_dict['asof_date'] = soup.find('repPdDate').text.strip() if soup.find('repPdDate') else ''
        doc_dict['cik_number'] = int(soup.find('cik').text) if soup.find('cik') else ''
        doc_dict['series_name'] = soup.find('seriesName').text.strip() if soup.find('seriesName') else ''
        doc_dict['total_assets'] = float(soup.find('totAssets').text) if soup.find('totAssets') else '0.0'
        doc_dict['net_assets'] = float(soup.find('netAssets').text) if soup.find('netAssets') else '0.0'

        seriesId = soup.find('seriesId').text.strip() if soup.find('seriesId') else ''
        doc_dict['series_number'] = int(seriesId[1:]) if (seriesId.startswith('S')) else ''

        tickers = soup.find_all('CLASS-CONTRACT-TICKER-SYMBOL')
        doc_dict['series_tickers'] = [ticker.text.strip() for ticker in tickers]

        invstOrSecs = soup.find_all('invstOrSec')
        doc_dict['holdings'] = []

        for invstOrSec in invstOrSecs:
            holding = {}
            holding['holding_name'] = invstOrSec.find('name').text.strip() if invstOrSec.find('name') else 'N/A'
            holding['holding_title'] = invstOrSec.find('title').text.strip() if invstOrSec.find('title') else 'N/A'
            holding['holding_share'] = float(invstOrSec.find('balance').text) if invstOrSec.find('balance') else 0.0
            holding['holding_value'] = float(invstOrSec.find('valUSD').text) if invstOrSec.find('valUSD') else 0.0
            holding['holding_type'] = invstOrSec.find('assetCat').text.strip() if invstOrSec.find('assetCat') else 'OTHER'
            doc_dict['holdings'].append(holding)

    return doc_dict


# download a filing if it doesn't exist
def download_filing(filing_url):
    file_name = 'master.{}.idx'.format(date)
    file = os.sep.join(['.', 'input', 'master.{}.idx'.format(date)])
    err = False

    # check if the file exists, so we don't need to request it again.
    if not os.path.exists(file):
        # file does not exist, download...
        file_url = make_url(base_url, [year, qtr, file_name])
        resp = requests.get(file_url)
        if resp.status_code == 200:
            print("Downloaded ", file_url)

            # we can always write the content to a file, so we don't need to request it again.
            with open(file, 'wb') as f:
                f.write(resp.content)
        else:
            print("Failed to download ", file_url)
            print(resp)
            err = True

    return err, file


# define a function to download the master index file
def download_master_index_file(date):
    file_name = 'master.{}.idx'.format(date)
    file = os.sep.join(['.', 'input', 'master.{}.idx'.format(date)])
    err = False

    # check if the file exists, so we don't need to request it again.
    if not os.path.exists(file):
        # file does not exist, download...
        file_url = make_url(base_url, [year, qtr, file_name])
        resp = requests.get(file_url)
        if resp.status_code == 200:
            print("Downloaded ", file_url)

            # we can always write the content to a file, so we don't need to request it again.
            with open(file, 'wb') as f:
                f.write(resp.content)
        else:
            print("Failed to download ", file_url)
            print(resp)
            err = True

    return err, file


# define a function to process master index file
def process_master_index_file(file):
    # read the master index file into a pandas dataframe
    df = pd.read_csv(file, delimiter='|', skiprows=5, parse_dates=['Date Filed'])
    df_nport = df[df['Form Type'] == 'NPORT-P']

    # parse NPORT forms
    base_url = r"https://www.sec.gov/Archives"
    for index, row in df_nport.iterrows():
        filing_url = make_url(base_url, [row['File Name']])

        # check if the filings has been processed already
        if not processed_filings_table.contains(Query().url == filing_url):
            print("Filing not processed, downloading...", filing_url)
            nport_dict = parse_nport_form(filing_url)
            print("Parsed ", filing_url)

            # add the filing date
            nport_dict['filing_date'] = row['Date Filed'].strftime('%Y-%m-%d')

            # add the company name
            nport_dict['company_name'] = row['Company Name']

            # flag the filings as processed
            processed_filings_table.insert({'url': filing_url})

            # return the dictionary object
            yield nport_dict

# define a function to save NPORT form in JSON file format
def save_as_json_file(nport_data):
    # save each NPORT form data into it's own file
    file_name = 'NPORT-P_{}_{}_{}.json'.format(nport_data['filing_date'],
                                              nport_data['company_name'],
                                              nport_data['series_name'])
    file = os.sep.join(['.', 'output', file_name])
    with open(file, 'w') as f:
        json.dump(nport_data, f)


# define a function to save NPORT form data in Ray Meadows file format
def save_as_ray_meadows_file(nport_data):
    # save each NPORT form data into it's own file
    file_name = 'NPORT-P_{}_{}_{}.csv'.format(nport_data['filing_date'],
                                              nport_data['company_name'],
                                              nport_data['series_name'])
    file = os.sep.join(['.', 'output', file_name])
    with open(file, 'w') as f:
        # write the first header row
        # As of Date	Filing Date	CIK Number	Series Number	Series name	Total Assets	Total net Assets	Series Ticker1
        header_1 = ['As of Date',
                    'Filing Date',
                    'CIK Number',
                    'Series Number',
                    'Series name',
                    'Total Assets',
                    'Total net Assets'
                    ] + ['Series Ticker{}'.format(i + 1) for i in range(len(nport_data['series_tickers']))]
        f.write('|'.join(header_1))
        f.write('\n')

        # write the first value row for the header
        # 2020-04-30	2020-06-26	877232	7715	Green Century Equity Fund	317251629	318112687	318798341	GCEQX
        row_1 = [nport_data['asof_date'],
                 nport_data['filing_date'],
                 str(nport_data['cik_number']),
                 str(nport_data['series_number']),
                 nport_data['series_name'],
                 str(nport_data['total_assets']),
                 str(nport_data['net_assets'])
                 ] + [ticker for ticker in nport_data['series_tickers']]
        f.write('|'.join(row_1))
        f.write('\n')

        # write the second header row
        # Filing Classification	Holding Type	Holding Name	Holding Share	Holding Value	Holding Face Amt	Holding Number Of Contracts	Future Gain Or Loss
        header_2 = 'Holding Name|Holding Share|Holding Value'
        f.write(header_2)
        f.write('\n')

        # write the holdings rows
        for holding in nport_data['holdings']:
            row = '{}|{}|{}'.format(
                holding['holding_title'] if holding['holding_name'] == 'N/A' else holding['holding_name'],
                holding['holding_share'],
                holding['holding_value'])
            f.write(row)
            f.write('\n')
        print("Created ", file_name)


# define a function to save NPORT form data in csv file format
def save_as_csv_file(file, nport_data):
    with open(file, 'w') as f:
        # write the header row
        # Holding Type	Holding Name	Holding Share	Holding Value
        header = 'Holding Name,Holding Share,Holding Value'
        f.write(header)
        f.write('\n')

        # write the holdings rows
        for holding in nport_data['holdings']:
            row = '"{}",{},{}'.format(
                holding['holding_title'] if holding['holding_name'] == 'N/A' else holding['holding_name'],
                round(holding['holding_share']),
                round(holding['holding_value']))
            f.write(row)
            f.write('\n')


# configure the parameters to build the master index file url
base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
year = '2020'
qtr = 'QTR3'
dates = ['20200728', '20200727']

# download master index files for the given dates
for date in dates:
    err, idx_file = download_master_index_file(date)
    if not err:
        # process the master index file
        for data in process_master_index_file(idx_file):
            # save each NPORT form data into it's own file
            file_name = 'NPORT-P_{}_{}_{}.csv'.format(data['filing_date'],
                                                      data['company_name'],
                                                      data['series_name'])
            file = os.sep.join(['.', 'output', clean_filename(file_name)])

            save_as_csv_file(file, data)
            print("Created ", file)
            time.sleep(1)

db.close()
