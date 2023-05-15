import dimensions as dim
import sqlite3
import pandas as pd


if __name__ == "__main__":

    con = sqlite3.connect('../data/globi.db')

    cur = con.cursor()

    list_sources = cur.execute("select distinct sourceDOI from interactions where (interactionTypeName == 'pollinates' or interactionTypeName == 'visitsFlowersOf' or interactionTypeName == 'pollinatedBy' )")
    

    results = pd.DataFrame(columns=['hasDOI','license','datatype'])


    for row in list_sources:

        url = "https://doi.org/" + row[0]
        try:
            metadata_box = dim.extract_metadata(url)

            info = dim.get_dictionary_by_key_value(metadata_box, '@type', 'Dataset')

            try:
                license = info['license']
            except:
                license = 'unknown'

            try:
                dist = info['distribution'][0]
                datatype = dist['encodingFormat']
            except:
                datatype = 'unknown'

            results = results.append({'hasDOI' : 'True', 'license' : license, 'datatype' : datatype}, ignore_index=True)


        except Exception as e:
            print(row[0])
            results = results.append({'hasDOI' : 'False', 'license' : 'unknown', 'datatype' : 'unknown'}, ignore_index=True)

    print(results)
