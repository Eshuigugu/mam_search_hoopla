import time
import requests
import json
from bs4 import BeautifulSoup
import os
import pickle
from appdirs import user_data_dir

# this script does create some files under this directory
appname = "search_hoopla"
appauthor = "Eshuigugu"
data_dir = user_data_dir(appname, appauthor)


if not os.path.isdir(data_dir):
    os.makedirs(data_dir)
sess_filepath = os.path.join(data_dir, 'session.pkl')

mam_blacklist_filepath = os.path.join(data_dir, 'blacklisted_ids.txt')
if os.path.exists(mam_blacklist_filepath):
    with open(mam_blacklist_filepath, 'r') as f:
        blacklist = set([int(x.strip()) for x in f.readlines()])
else:
    blacklist = set()

if os.path.exists(sess_filepath):
    sess = pickle.load(open(sess_filepath, 'rb'))
    # only take the cookies
    cookies = sess.cookies
    sess = requests.Session()
    sess.cookies = cookies
else:
    sess = requests.Session()


def search_hoopla(title, authors, category_str):
    media_items = []
    hoopla_endpoint_url = 'https://patron-api-gateway.hoopladigital.com/graphql'
    # KindId 5 is for ebooks, 8 is for audiobooks, 10 for comics
    if category_str == 'Ebooks':
        category_int = 5
    elif category_str == 'Audiobooks':
        category_int = 8
    elif category_str == 'Comics':
        category_int = 10
    else:
        # if category_str not in ['Ebooks', 'Audiobooks', 'Comics']
        print(f'skipping book with unrecognized category {category_str} {title} {authors}')
        return []
    for author in authors[:5]:
        json_payload = {'operationName': 'FilterSearch',
                        'variables': {'criteria': {"title": title, "artistName": author, 'kindId': str(category_int), 'availability': 'ALL_TITLES',
                                                   'pagination': {'page': 1, 'pageSize': 48}}},
                        'query': r'''query FilterSearch($criteria: SearchCriteria!, $sort: Sort) {search(criteria: $criteria, sort: $sort) {    found    hits {      ...TitleListItemFragment      __typename    }    aggregations {      ...AggregationsFragment      __typename    }    algorithm    __typename  }}fragment AggregationsFragment on Facet {  name  buckets {    key    value    __typename  }  __typename}fragment TitleListItemFragment on Title {  id  artKey  issueNumberDescription  lendingMessage  kind {    name    __typename  }  parentalAdvisory  primaryArtist {    name    __typename  }  releaseDate  title  titleId  status  licenseType  __typename}'''}
        try:
            r = sess.post(hoopla_endpoint_url, json=json_payload, headers={'content-type': 'application/json'})
        except requests.ConnectionError as e:
            print(f'error {e}')
            time.sleep(10)
            continue
        time.sleep(1)
        r_json = r.json()

        if r.status_code == 200 and r_json['data']['search']['hits']:
            for hoopla_item in r_json['data']['search']['hits']:
                hoopla_item['url'] = f'https://www.hoopladigital.com/title/{hoopla_item["id"]}'
            media_items += r_json['data']['search']['hits']
    # ensure each result is unique
    media_items = list({x['url']: x for x in media_items}.values())
    return media_items


def get_mam_requests(limit=5000):
    keepGoing = True
    start_idx = 0
    req_books = []

    # fetch list of requests to search for
    while keepGoing:
        time.sleep(1)
        url = 'https://www.myanonamouse.net/tor/json/loadRequests.php'
        headers = {}
        # fill in mam_id for first run
        # headers['cookie'] = 'mam_id='

        query_params = {
            'tor[text]': '',
            'tor[srchIn][title]': 'true',
            'tor[viewType]': 'unful',
            'tor[startDate]': '',
            'tor[endDate]': '',
            'tor[startNumber]': f'{start_idx}',
            'tor[sortType]': 'dateD'
        }
        headers['Content-type'] = 'application/json; charset=utf-8'

        r = sess.get(url, params=query_params, headers=headers)
        if r.status_code >= 300:
            raise Exception(f'error fetching requests. status code {r.status_code} {r.text}')

        req_books += r.json()['data']
        total_items = r.json()['found']
        start_idx += 100
        keepGoing = min(total_items, limit) > start_idx and not \
            {x['id'] for x in req_books}.intersection(blacklist)

    # saving the session lets you reuse the cookies returned by MAM which means you won't have to manually update the mam_id value as often
    with open(sess_filepath, 'wb') as f:
        pickle.dump(sess, f)

    with open(mam_blacklist_filepath, 'a') as f:
        for book in req_books:
            f.write(str(book['id']) + '\n')
            book['url'] = 'https://www.myanonamouse.net/tor/viewRequest.php/' + \
                          str(book['id'])[:-5] + '.' + str(book['id'])[-5:]
            book['title'] = BeautifulSoup(book["title"], features="lxml").text
            book['authors'] = [author for k, author in json.loads(book['authors']).items()]
    return req_books


def main():
    req_books = get_mam_requests()

    req_books_reduced = [x for x in req_books if
                         (x['cat_name'].startswith('Ebooks ') or x['cat_name'].startswith('Audiobooks '))
                         and x['filled'] == 0
                         and x['torsatch'] == 0
                         and x['id'] not in blacklist]
    for book in req_books_reduced:
        # category will be Ebooks, Audiobooks, or Comics
        # skip newspapers/magazines
        if book['category'] == 79:
            continue
        elif book['category'] == 61:
            category = 'Comics'
        else:
            category = book['cat_name'].split(' ')[0]

        hits = search_hoopla(book['title'], book['authors'], category)
        if hits:
            print(book['title'])
            print(' ' * 2 + book['url'])
            if len(hits) > 5:
                print(' ' * 2 + f'got {len(hits)} hits')
                print(' ' * 2 + f'showing first 5 results')
                hits = hits[:5]
            for hit in hits:
                print(' ' * 2 + hit['title'])
                print(' ' * 4 + hit['url'])
            print()


if __name__ == '__main__':
    main()

