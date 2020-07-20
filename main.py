# import our libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
from flask import escape

# let's first make a function that will make the process of building a url easy.
def make_url(base_url, comp):
    url = base_url

    # add each component to the base url
    for r in comp:
        url = '{}/{}'.format(url, r)

    return url

def get_master_index_file(request):
    request_args = request.args

    if request_args and 'date' in request_args:
        date = request_args['date']
    else:
        date = '20200717'

    # configure the parameters to build the master index file url
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
    year = '2020'
    qtr = 'QTR3'

    # download the master index file
    file_url = make_url(base_url, [year, qtr, 'master.{}.idx'.format(date)])
    resp = requests.get(file_url)
    if resp.status_code == 200:
        return 'Downloaded {}!'.format(escape(file_url))
    else:
        return 'Download Failed!'
