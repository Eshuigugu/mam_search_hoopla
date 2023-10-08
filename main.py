import time
import requests
import json
import os
import pickle
from appdirs import user_data_dir
import html
import csv
import argparse
from typing import List

# this script does create some files under this directory
appname = "search_hoopla"
appauthor = "Eshuigugu"
data_dir = user_data_dir(appname, appauthor)
cookies_filepath = os.path.join(data_dir, 'cookies.pkl')
resume_id_filepath = os.path.join(data_dir, 'resume_id.txt')

if not os.path.isdir(data_dir):
    os.makedirs(data_dir)

if os.path.exists(resume_id_filepath):
    with open(resume_id_filepath, 'r') as f:
        resume_id = max([int(x.strip()) for x in f.readlines()])
else:
    resume_id = 0

sess = requests.Session()
if os.path.exists(cookies_filepath):
    cookies = pickle.load(open(cookies_filepath, 'rb'))
    sess.cookies = cookies


def search_hoopla(title: str, authors: List[str], category_str: str, subtitle: str = None) -> List[dict]:
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
                        'variables': {'criteria': {"title": title, "artistName": author, 'kindId': str(category_int),
                                                   'availability': 'ALL_TITLES',
                                                   'pagination': {'page': 1, 'pageSize': 48}}},
                        'query': r'''query FilterSearch($criteria: SearchCriteria!, $sort: Sort) {search(criteria: $criteria, sort: $sort) {    found    hits {      ...TitleListItemFragment      __typename    }    aggregations {      ...AggregationsFragment      __typename    }    algorithm    __typename  }}fragment AggregationsFragment on Facet {  name  buckets {    key    value    __typename  }  __typename}fragment TitleListItemFragment on Title {  id  artKey  issueNumberDescription  lendingMessage  kind {    name    __typename  }  parentalAdvisory  primaryArtist {    name    __typename  }  releaseDate  title  subtitle  titleId  status  licenseType  __typename}'''}
        try:
            r = sess.post(hoopla_endpoint_url, json=json_payload, headers={'content-type': 'application/json'})
        except requests.ConnectionError as e:
            print(f'error {e}')
            time.sleep(10)
            continue
        time.sleep(1)

        if r.status_code == 200:
            try:
                r_json = r.json()
            except json.decoder.JSONDecodeError:
                print('error loading reponse JSON', r.text)
                continue
            if r_json['data']['search']['hits']:
                for hoopla_item in r_json['data']['search']['hits']:
                    hoopla_item['url'] = f'https://www.hoopladigital.com/title/{hoopla_item["id"]}'
                    if hoopla_item['subtitle']:
                        hoopla_item['title'] += f": {hoopla_item['subtitle']}"
                    if subtitle and subtitle.lower() not in hoopla_item['title'].lower():
                        continue
                    media_items.append(hoopla_item)
        else:
            print('bad response', r, r.text[:100])
    # ensure each result is unique
    media_items = list({x['url']: x for x in media_items}.values())
    return media_items


def input_mam_id():
    mam_id = input(f'provide mam_id: ').strip()
    headers = {"cookie": f"mam_id={mam_id}"}
    r = sess.get('https://www.myanonamouse.net/jsonLoad.php', headers=headers, timeout=5)  # test cookie
    if r.status_code != 200:
        raise Exception(f'error fetching requests. status code {r.status_code} {r.text}')


def search_mam(title, author, audiobook=False, ebook=False):
    mam_categories = []
    if audiobook:
        mam_categories.append(13)
    if ebook:
        mam_categories.append(14)
    if not mam_categories:
        return False
    params = {
        "tor": {
            "text": f"@title {title} @author {author}",  # The search string.
            "main_cat": mam_categories
        },
    }
    try:
        r = sess.post('https://www.myanonamouse.net/tor/js/loadSearchJSONbasic.php', json=params)
        if r.text == '{"error":"Nothing returned, out of 0"}':
            return False
        if r.json()['total']:
            return f"https://www.myanonamouse.net/t/{r.json()['data'][0]['id']}"
    except Exception as e:
        print(f'error searching MAM {e}')
    return False


def get_mam_requests(limit: int = 5000) -> List[dict]:
    keep_going = True
    start_idx = 0
    req_books = []

    # fetch list of requests to search for
    while keep_going:
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

        r = sess.get(url, params=query_params, headers=headers, timeout=60)
        if r.status_code >= 300:
            print(f'error fetching requests. status code {r.status_code} {r.text}')
            if r.status_code == 403:
                input_mam_id()

        req_books += r.json()['data']
        total_items = r.json()['found']
        start_idx += 100
        # check that it's not returning requests already searched for
        keep_going = min(total_items, limit) > start_idx and not \
            min(book["id"] for book in req_books) <= resume_id

    # save cookies for later
    with open(cookies_filepath, 'wb') as f:
        pickle.dump(sess.cookies, f)

    with open(resume_id_filepath, 'w') as resume_file:
        # arrange list of requests old > new
        for book in req_books[::-1]:
            # write the most recent request id
            resume_file.seek(0)
            resume_file.write(str(book["id"]))
            # edit book object
            book['url'] = f'https://www.myanonamouse.net/tor/viewRequest.php/{book["id"] / 1e5:.5f}'
            book['title'] = html.unescape(str(book['title']))
            if book['authors']:
                book['authors'] = [author for k, author in json.loads(book['authors']).items()]
            if book["id"] > resume_id:
                yield book


def should_search_for_book(mam_book: dict) -> bool:
    return (mam_book['cat_name'].startswith('Ebooks ') or mam_book['cat_name'].startswith('Audiobooks ')) \
           and mam_book['filled'] == 0 \
           and mam_book['torsatch'] == 0 \
           and mam_book['category'] != 79


def search_for_mam_book(mam_book: dict) -> List[dict]:
    # category will be Ebooks, Audiobooks, or Comics
    # skip newspapers/magazines
    if mam_book['category'] == 61:
        category = 'Comics'
    else:
        category = mam_book['cat_name'].split(' ')[0]

    try:
        if ': ' in mam_book['title']:
            title, subtitle = mam_book['title'].split(': ', maxsplit=1)
            return search_hoopla(title, mam_book['authors'], category, subtitle=subtitle)
        else:
            return search_hoopla(mam_book['title'], mam_book['authors'], category)
    except Exception as e:
        print('error', e)
        return []


def pretty_print_hits(mam_book: dict, hits: List[dict]) -> None:
    print(mam_book['title'])
    print(' ' * 2 + mam_book['url'])
    if len(hits) > 5:
        print(' ' * 2 + f'got {len(hits)} hits')
        print(' ' * 2 + f'showing first 5 results')
        hits = hits[:5]
    for hit in hits:
        print(' ' * 2 + hit["title"])
        print(' ' * 4 + hit['url'])
    print()


def write_to_csv(csv_filepath: str, book: dict, hits: List[dict]):
    query_str = f'{book["title"]} {book["authors"][0]}'
    try:
        r = sess.get('https://www.goodreads.com/book/auto_complete', params={'format': 'json', 'q': query_str},
                     timeout=10)
    except Exception as e:
        print('error querying goodreads', e)
        r = None
    if r.status_code == 200 and r.json():
        goodreads_book = r.json()[0]
    else:
        goodreads_book = {}
    goodreads_book_url = f'https://www.goodreads.com{goodreads_book["bookUrl"]}' if "bookUrl" in goodreads_book else ""
    goodreads_num_ratings = goodreads_book.get("ratingsCount", "")

    on_mam = search_mam(book["title"], book["authors"][0],
                        ebook=book['cat_name'].startswith('Ebooks '),
                        audiobook=book['cat_name'].startswith('Audiobooks ')
                        )
    # add series
    book_data = {
        "url": book["url"],
        "title": book["title"],
        "authors": ", ".join(book["authors"]),
        "series": html.unescape(" #".join(list(json.loads(book["series"]).values())[0])) if book["series"] else "",
        "votes": book["votes"],
        "category": book["cat_name"],
        "found_urls": " ".join([hit["url"] for hit in hits]),
        "found_title": hits[0]["title"],
        "goodreads_url": goodreads_book_url,
        "num_ratings": goodreads_num_ratings,
        "on_mam": on_mam,
    }
    write_headers = not os.path.exists(csv_filepath)
    with open(csv_filepath, mode="a", newline="", errors='ignore') as csv_file:
        fieldnames = book_data.keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if write_headers:
            writer.writeheader()
        writer.writerow(book_data)


def main():
    parser = argparse.ArgumentParser(description="Append data to a CSV file.")
    parser.add_argument("--output_file", help="Path to the CSV output file (optional)")
    args = parser.parse_args()
    for book in filter(should_search_for_book, get_mam_requests()):
        hits = search_for_mam_book(book)
        if hits:
            pretty_print_hits(book, hits)
            if args.output_file:
                write_to_csv(csv_filepath=args.output_file, book=book, hits=hits)


if __name__ == '__main__':
    main()

