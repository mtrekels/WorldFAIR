import pandas as pd
import extruct
import requests
from w3lib.html import get_base_url
from urllib.parse import urlparse
import matplotlib.pyplot as plt
from collections import Counter

def extract_metadata(url):

    r = requests.get(url)
    base_url = get_base_url(r.text, r.url)
    metadata = extruct.extract(r.text, 
                                base_url=base_url,
                                uniform=True,
                                syntaxes=['json-ld',
                                    'microdata',
                                    'opengraph'])
    return metadata

def get_dictionary_by_key_value(dictionary, target_key, target_value):

    for key in dictionary:
        if len(dictionary[key]) > 0:
            for item in dictionary[key]:
                if item[target_key] == target_value:
                    return item


def import_dimensions(infile):
    df = pd.read_csv(infile, header=1)
    return df

if __name__ == '__main__':

    data = import_dimensions('../data/Dimensions_search.csv')

    urls = data['Source linkout']

    license = []
    license2 = []
    datatype = []
    has_doi = []

    for url in urls:
        metadata_box = extract_metadata(url)
        print(metadata_box)
        info = get_dictionary_by_key_value(metadata_box, '@type', 'Dataset')

        try:
            license.append(info['license'])
        except:
            license.append('unknown')

        try:
            dist = info['distribution'][0]
            datatype.append(dist['encodingFormat'])
        except:
            datatype.append('unknown')

        try:
            if 'doi' in info['identifier']:
                has_doi.append('Y')
            else:
                has_doi.append('N')
        except:
            has_doi.append('Unknown')

    print(license)

    type_count = Counter(datatype)
    types = pd.DataFrame.from_dict(type_count, orient='index')
    fig = types.plot(kind='bar')
    fig.get_figure().savefig('../results/output.png', format='png')
