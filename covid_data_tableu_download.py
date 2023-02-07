import requests
from bs4 import BeautifulSoup
import json
import re
import time
import csv
import os
import hashlib


INDEX_FILE = './index.txt'
RESULT_FILE = './result.txt'
SUMMARY_FILE = './summary.txt'
DOWNLOAD_FOLDER = './downloads/'
READ_FILE = 'r'
READ_BINARY = 'rb'
WRITE_FILE = 'w'


files = {
    "file2.csv": {
        "url": "https://public.tableau.com/views/OverviewDashboard_15852499073250/DashboardOverview_1?:embed=y&:showVizHome=no",
        "date_regex": r"(\d{1,2}\-\d{1,2}\-\d{1,2})"
    },
}


def download_process():
    for file_name, parameters in files.items():
        my_rows = [['Date', 'Cases', 'Presumed Recovered']]
        recovered = None
        my_date = None

        response = requests.get(
            parameters['url'],
            params={
                ":showVizHome": "no",
            }
        )

        soup = BeautifulSoup(response.text, "html.parser")
        tableau_data = json.loads(soup.find("textarea", {"id": "tsConfigContainer"}).text)

        data_url = 'https://public.tableau.com{}/bootstrapSession/sessions/{}' \
            .format(tableau_data["vizql_root"], tableau_data["sessionid"])

        response = requests.post(data_url, data={
            "sheet_id": tableau_data["sheetId"]
        })

        time.sleep(0.5)

        data_match = re.search('\d+;({.*})\d+;({.*})', response.text, re.MULTILINE)
        data = json.loads(data_match.group(2))

        try:
            values = data["secondaryInfo"]["presModelMap"]["dataDictionary"]["presModelHolder"]["genDataDictionaryPresModel"][
                "dataSegments"]["0"]["dataColumns"]
        except KeyError:
            print('Values not found!!!')
            continue

        for el in values:
            # Value logic
            if el['dataType'] == 'integer':
                vals = el['dataValues']
                cases = None
                for ind, val in enumerate(vals):
                    if recovered:
                        break

                    curr_val = int(val)
                    '''
                    The logic:
                    The query returns all values from the page in a single list. 
                    The value we need is cumulative and will always be bigger than 1000000.
                    The first such value in the list is 'Cases' and we need to capture the second.
                    '''
                    if not cases and curr_val > 1000000:
                        cases = curr_val
                        print('Cases: {}'.format(str(cases)))
                    elif curr_val > 1000000:
                        try:
                            '''
                            We do this check in order to avoid capturing incorrect number
                            Currently the following value is daily data and should be smaller
                            '''
                            next_val = vals[ind+1]
                            if next_val > 1000000:
                                print('The values sequence has changed!!!')
                                break
                            recovered = curr_val
                        except IndexError:
                            continue

                        print('Recovered: {}'.format(str(recovered)))
            # Date logic
            elif el['dataType'] == 'cstring':
                vals = el['dataValues']

                for val in vals:
                    if re.search(parameters['date_regex'], val):
                        my_date = val
                        break

        if recovered and my_date:
            my_rows.append([my_date, cases, recovered])
        else:
            print('Value or date not found!!!')

        with open(DOWNLOAD_FOLDER + file_name, 'w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerows(my_rows)


# ================== Index and Summary logic ======================
# = check if the same file is downloaded again and delete it if so=
def delete_old_files():
    for the_file in os.listdir(DOWNLOAD_FOLDER):
        file_path = os.path.join(DOWNLOAD_FOLDER, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)

        except Exception as e:
            print(e)


def read_hash(file_path):
    try:
        with open('./downloads/{}'.format(file_path), 'rb') as fs:
            return hashlib.md5(fs.read()).hexdigest()
    except IOError:
        return 'Error'


def read_index():
    try:
        with open(INDEX_FILE, READ_FILE) as fs:
            text = fs.read()
            return json.loads(text)
    except (IOError, ValueError):
        with open(INDEX_FILE, WRITE_FILE) as fs:
            fs.write('{}')
            return {}


def write_index(json_text):
    try:
        with open(INDEX_FILE, WRITE_FILE) as fs:
            fs.write(json_text)
    except IOError:
        with open(INDEX_FILE, WRITE_FILE) as fs:
            return 'Error'


def write_result(count=0):
    try:
        with open(RESULT_FILE, WRITE_FILE) as fs:
            content = '{"downloaded": ' + str(count) + '}'
            fs.write(content)
    except IOError:
        return 'Error'


def prep_summary(_summary, _key, _filename):
    _downloaded = _summary.setdefault(_key, {})
    _downloaded[_filename] = 0
    return _summary


def write_summary(_summary):
    try:
        with open(SUMMARY_FILE, WRITE_FILE) as fs:
            fs.write(json.dumps(_summary))
    except IOError:
        pass


def run():
    try:
        os.mkdir(DOWNLOAD_FOLDER)
    except FileExistsError as e:
        print(str(e))

    delete_old_files()
    counter = 0
    summary = {}

    download_process()

    for _file in os.listdir('./downloads/'):
        print(_file, 'COUNT')

        try:
            hash = read_hash(_file)
            indexes = read_index()

            if _file in indexes:
                print(_file)
                # disable same status
                # if indexes[_file] == hash:
                if False:
                    print('SAME FILE')
                    os.remove('./downloads/' + _file)
                    summary = prep_summary(summary, 'same', _file)
                else:
                    indexes[_file] = hash
                    write_index(json.dumps(indexes))
                    summary = prep_summary(summary, 'downloaded', _file)
                    counter += 1
            else:
                indexes[_file] = hash
                write_index(json.dumps(indexes))
                summary = prep_summary(summary, 'downloaded', _file)
                counter += 1
        except IOError:
            summary = prep_summary(summary, 'error', _file)
    write_summary(summary)
    write_result(counter)


if __name__ == '__main__':
    run()
