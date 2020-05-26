__author__ = 'Ahmed G. Ali'
# initial data
IDR_BASE_URL = "https://idr.openmicroscopy.org"
import requests

INDEX_PAGE = "%s/webclient/?experimenter=-1" % IDR_BASE_URL

# create http session
with requests.Session() as session:
    request = requests.Request('GET', INDEX_PAGE)
    prepped = session.prepare_request(request)
    response = session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()

MAP_URL = "{base}/webclient/api/annotations/?type=map&{type}={screen_id}"

SCREEN_ID = 154

qs = {'base': IDR_BASE_URL, 'type': 'screen', 'screen_id': SCREEN_ID}
url = MAP_URL.format(**qs)
for a in session.get(url).json()['annotations']:
    namespace = a['ns']
    for v in a['values']:
        key = v[0]
        value = v[1]
        print (key, value)