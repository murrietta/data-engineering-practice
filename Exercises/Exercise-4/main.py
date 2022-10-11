import os
import json
import csv
import logging
log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
log_kwargs = {'level': logging.INFO, 'filemode': 'w',
                'format': log_format, 'force': True}
logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)


folder = './data/'


def main():
    # us os.walk to see all files in target folder and it's subfolders
    logger.info(f'Finding all json files in {folder}')
    json_paths = [os.path.join(subdir, file)
            for subdir, dirs, files in os.walk(folder)
            for file in files if file.endswith('.json')]
    # or could use glob
    # import glob
    # json_paths = glob.glob('./data/**/*.json', recursive=True)

    # load each
    files = []
    for i, json_path in enumerate(json_paths):
        try:
            logger.info(f'Acquiring content from {json_path}')
            with open(json_path, 'r') as f:
                data = json.loads(f.read())
        except Exception as e:
            logger.error(f'Failed to load {json_path}\n{e}')
            data = None
        # add to files
        files.append({'path': json_path, 'data': data})
    
    # flatten geolocation and write to csv in the same directory as json file
    # should probably use flatten_json for this but hand-roll today
    # limitation is that we are assuming a specific format
    for i, file in enumerate(files):
        # flatten, save back to files object as new key
        logger.info(f'Flattening json {file["path"]}')
        try:
            flatdata = file['data'].copy()
            flatdata['geolocation.type'] = flatdata['geolocation']['type']
            flatdata['geolocation.coordinates.0'] = flatdata['geolocation']['coordinates'][0]
            flatdata['geolocation.coordinates.1'] = flatdata['geolocation']['coordinates'][1]
            geo = flatdata.pop('geolocation')
            files[i].update({'flatdata': flatdata})
        except Exception as e:
            logger.error(f'Failed to flatten json\n{e}')
            continue
        # write to csv in same folder as json
        csvpath = file['path'].replace('.json', '.csv')
        logger.info(f'Writing json as csv to {csvpath}')
        try:
            field_names = list(flatdata.keys())
            with open(csvpath, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                writer.writerows([flatdata])  # as array for writerows
        except Exception as e:
            logger.error(f'Failed to write as csv\n{e}')
            continue


if __name__ == '__main__':
    main()
