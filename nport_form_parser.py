# import our libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup

# let's first make a function that will make the process of building a url easy.
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
        holding['holding_name'] = invstOrSec.find('title').text.strip() if invstOrSec.find('title') else 'N/A'
        holding['holding_share'] = float(invstOrSec.find('balance').text) if invstOrSec.find('balance') else 0.0
        holding['holding_value'] = float(invstOrSec.find('valUSD').text) if invstOrSec.find('valUSD') else 0.0
        holding['holding_type'] = invstOrSec.find('assetCat').text.strip() if invstOrSec.find('assetCat') else 'OTHER'
        doc_dict['holdings'].append(holding)

    return doc_dict

# configure the parameters to build the master index file url
base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
year = '2020'
qtr = 'QTR3'
date = '20200718'

# download the master index file
file_url = make_url(base_url, [year, qtr, 'master.{}.idx'.format(date)])
resp = requests.get(file_url)
if resp.status_code == 200:
    print("Downloaded ", file_url)

    # we can always write the content to a file, so we don't need to request it again.
    file = './input/master_{}.csv'.format(date)
    with open(file, 'wb') as f:
         f.write(resp.content)

    # read the master index file into a pandas dataframe
    df = pd.read_csv(file, delimiter='|', skiprows=5, parse_dates=['Date Filed'])
    df_nport = df[df['Form Type'] == 'NPORT-P']

    # create a dictionary to hold the parsed NPORT form data
    base_url = r"https://www.sec.gov/Archives"

    nports_dict = {}
    for index, row in df_nport.iterrows():
        filing_url = make_url(base_url, [row['File Name']])
        nport_data = parse_nport_form(filing_url)
        print("Parsed ", filing_url)

        # add the filing date
        nport_data['filing_date'] = row['Date Filed'].strftime('%Y-%m-%d')

        # create a key to store the parsed data dictionary
        key = 'NPORT-P_{}_{}_{}'.format(row['Company Name'], nport_data['series_name'], nport_data['filing_date'])
        nports_dict[key] = nport_data

    # save each NPORT form data into it's own file
    for key, val in nports_dict.items():
        # file name based on the key
        file_name = './output/{}.csv'.format(key)

        with open(file_name, 'w') as f:
            # write the first header row
            # As of Date	Filing Date	CIK Number	Series Number	Series name	Total Stocks Value	Total Assets	Total net Assets	Series Ticker1
            header_1 = ['As of Date',
                        'Filing Date',
                        'CIK Number',
                        'Series Number',
                        'Series name',
                        'Total Stocks Value',
                        'Total Assets',
                        'Total net Assets'
                        ] + ['Series Ticker{}'.format(i + 1) for i in range(len(val['series_tickers']))]
            f.write('|'.join(header_1))
            f.write('\n')

            # write the first value row for the header
            # 2020-04-30	2020-06-26	877232	7715	Green Century Equity Fund	317251629	318112687	318798341	GCEQX
            row_1 = [val['asof_date'],
                     val['filing_date'],
                     str(val['cik_number']),
                     str(val['series_number']),
                     val['series_name'],
                     '',
                     str(val['total_assets']),
                     str(val['net_assets'])
                     ] + [ticker for ticker in val['series_tickers']]
            f.write('|'.join(row_1))
            f.write('\n')

            # write the second header row
            # Filing Classification	Holding Type	Holding Name	Holding Share	Holding Value	Holding Face Amt	Holding Number Of Contracts	Future Gain Or Loss
            header_2 = 'Filing Classification|Holding Type|Holding Name|Holding Share|Holding Value|Holding Face Amt|Holding Number Of Contracts|Future Gain Or Loss'
            f.write(header_2)
            f.write('\n')

            # write the holdings rows
            for holding in val['holdings']:
                row = '{}|{}|{}|{}|{}|0|0|0'.format(holding['holding_type'],
                                                    holding['holding_type'],
                                                    holding['holding_name'],
                                                    holding['holding_share'],
                                                    holding['holding_value']
                                                    )
                f.write(row)
                f.write('\n')
            print("Created ", file_name)
else:
    print("Failed to download ", file_url)
    print(resp)