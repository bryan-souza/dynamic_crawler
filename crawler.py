import re
import json
import requests

from rich import print
from typing import Dict
from bs4 import BeautifulSoup
from multiprocessing import Pool

def get_db_client( options: Dict, **kwargs ):
    from pymongo import MongoClient

    options = {**options, **kwargs}
    uri = f"mongodb://{options['username']}:{options['password']}@{options['host']}:{options['port']}/{options['database']}"

    return MongoClient( uri )

def validator(obj, path):
    f = lambda x: f"['{x}']"

    string = ""
    for x in path.split('.'):
        string += f(x)

    try:
        return eval( f"obj{string}" )
    except:
        return None

def crawler(url, enable_db=False):
    db = get_db_client(
        json.load( open('config.json') )['db']
    ) 
    collection = db['real_estate']

    print( f"Started crawling {url}" )

    while True:
        # Setup
        page = requests.get( url, headers=headers )
        soup = BeautifulSoup(page.content, 'html.parser')

        # Scrape links from listing
        links = parse_listing_urls(url)
        data = [ page_scraper(link) for link in links ]

        # Scrape pages & store in db
        if (enable_db):
            collection.insert_many(data)
        else:
            print(data)

        # Break loop if last page
        next = soup.find('a', {'data-lurker-detail': "next_page"})
        if ( next is None ):
            break

        # Change link for crawling
        url = next['href']

def parse_listing_urls(url):
    page = requests.get( url, headers=headers )
    soup = BeautifulSoup(page.content, 'html.parser')

    links = soup.find_all('a', {'data-lurker-detail': 'list_id'})
    for link in links:
        yield link['href']

def page_scraper(url):
    page = requests.get( url, headers=headers )
    soup = BeautifulSoup(page.content, 'html.parser')

    txt = soup.find( 'script', string=re.compile("window.dataLayer") ).text
    obj = json.loads( txt[20:-1] )['page']

    return {
        'titulo': validator( obj, 'adDetail.subject' ),
        'regiao': validator( obj, 'adDetail.region' ),
        'categoria': validator( obj, 'adDetail.category' ),
        'tipo': validator( obj, 'adDetail.real_estate_type' ),
        'quartos': validator( obj, 'adDetail.rooms' ),
        'banheiros': validator( obj, 'adDetail.bathrooms' ),
        'preco': validator( obj, 'detail.price' )
    }

if __name__ == '__main__':
    # Setup
    base_url = "https://www.olx.com.br/imoveis"

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0'
    }

    # Gather states subdomain links
    page = requests.get( base_url, headers=headers )
    soup = BeautifulSoup(page.content, 'html.parser')

    states = soup.find_all('a', {'data-lurker-detail': 'linkshelf_item'})
    states = list( map(lambda x: x['href'], states) )

    # Create a pool of workers to scrape links
    pool = Pool()
    pool.map( crawler, states )
