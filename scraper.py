import requests
import csv
import sys
import json
from secret import app_token


def get_download_links(url, filename, path):
    keep_going = True
    offset = 0
    ids = []
    while keep_going:
        offset += 1
        print('Getting page ' + str(offset))
        offset_url = url + "&offset=" + str(offset)
        res = requests.get(offset_url)
        res = json.loads(res.content)
        res = res['results']

        if len(res) <= 0:
            keep_going = False
            break

        for table in res:
            name = table['resource']['name']
            table_id = table['resource']['id']
            my_obj = {'id': table_id, 'name': name}
            ids.append(my_obj)

    target_path = path + filename
    with open(target_path, 'w+') as f:
        for row in ids:
            # dname sometimes contains commas, so need to be put in quotes
            dname = '"' + row['name'] + '"'
            did = row['id']
            dformat = 'csv'
            durl = ('https://data.mass.gov/resource/' + did + '.csv' +
                    '$limit=1000000000$$app_token=' + app_token)

            to_write = ','.join((dname, did, dformat, durl)) + '\n'
            f.write(to_write)


def get_download_uniques(i_filename, o_filename):
    id_dict = {}
    with open(i_filename, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")
        for (dname, did, dformat, durl) in csvreader:
            id_dict[did] = (dname, did, dformat, durl)

    with open(o_filename, 'w+') as fil:
        for key, value in id_dict.items():

            dname = '"' + value[0] + '"'
            did = value[1]
            dformat = value[2]
            durl = value[3]

            to_write = ','.join((dname, did, dformat, durl)) + '\n'
            fil.write(to_write)


def download_files(o_path, input_csv, org):
    """
    Accepts an output path for files to be downloaded to
    Another path for a CSV containing download links
    And an string, Org, which is prepended to file names

    The input_csv is shoud be formatted as
    filename, file id, file format, file url
    """

    with open(input_csv, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")

        # with ThreadPoolExecutor(max_workers=12) as executor:
        for (dname, did, dformat, durl) in csvreader:

            filename = org + " " + dname + "_" + did + "." + dformat
            filename = filename.translate(str.maketrans({"/": r"\:"}))
            print(durl)
            download(durl, filename, o_path)
            # thread = executor.submit(download, durl, filename, path)
            # thread.result()


def download(url, filename, path):
    '''
        Downloads a file of filename from the URL and
        stores it in the path provided.
    '''

    print('download entered')
    res = None

    try:
        res = requests.get(url, stream=True)
        print('got request')
    except:
        print("ERROR while trying to download: " + str(url))
        return
    target_path = path + filename

    with open(target_path, 'wb+') as f:
        for chunk in res.iter_content(chunk_size=4096):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


if __name__ == "__main__":

    if sys.version_info < (3, 0):
        sys.stdout.write("Sorry, requires Python 3.x,\n")
        sys.exit(1)

    # get links for files and clean
    filename = "boston_links.csv"
    url = ("http://api.us.socrata.com/api/catalog/v1?" +
           "domains=data.cityofboston.gov&only=datasets")
    path = "./"
    get_download_links(url, filename, path)

    out_filename = "boston_cleaned_links.csv"
    get_download_uniques(filename, out_filename)

    filename = "cambridge_links.csv"
    url = ("http://api.us.socrata.com/api/catalog/v1?" +
           "domains=data.cambridgema.gov&only=datasets")
    path = "./"
    get_download_links(url, filename, path)
    out_filename = "cambridge_cleaned_links.csv"
    get_download_uniques(filename, out_filename)

    filename = "somerville_links.csv"
    url = ("http://api.us.socrata.com/api/catalog/v1?" +
           "domains=data.somervillema.gov&only=datasets")
    path = "./"
    get_download_links(url, filename, path)
    out_filename = "somerville_cleaned_links.csv"
    get_download_uniques(filename, out_filename)

    filename = "mass_links.csv"
    url = ("http://api.us.socrata.com/api/catalog/v1?" +
           "domains=data.mass.gov&only=datasets")
    path = "./"
    get_download_links(url, filename, path)
    out_filename = "mass_cleaned_links.csv"
    get_download_uniques(filename, out_filename)

    # download the data
    data_path = "./data/Somerville/"
    download_files(data_path, 'somerville_cleaned_links.csv', 'Somerville')

    data_path = "./data/Cambridge/"
    download_files(data_path, 'cambridge_cleaned_links.csv', 'Cambridge')

    data_path = "./data/Boston/"
    download_files(data_path, 'boston_cleaned_links.csv', 'Boston')

    data_path = "./data/Mass/"
    download_files(data_path, 'mass_cleaned_links.csv', 'Mass')
