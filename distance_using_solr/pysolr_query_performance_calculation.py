# Standard library
from urllib.request import urlopen
import json
import time
import random

def sample_floats(low, high, k = 1):
    """ Return a k-length list of unique random floats
        in the range of low <= x <= high
    """
    result = []
    seen = set()
    for _ in range(k):
        x = random.uniform(low, high)
        while x in seen:
            x = random.uniform(low, high)
        seen.add(x)
        result.append(round(x, 2))
    return result

def connect():
    
    y = sample_floats(-2.00, 2.00, k = 128)

    v = ''
    for j in range(0,128):
        v = v + 'v{}_f'.format(str(j)) + ','

    url = 'http://127.0.0.1:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,' + v + \
    '{}'.format(','.join(str(element) for element in y)) + ')&fl=image_link,euclidean_distance:$euclidean_distance&\
sort=$euclidean_distance%20asc&rows=5'

    t1 = time.perf_counter()
    connection = urlopen(url)
    t2 = time.perf_counter()

    print('Took {}s to calculate by query'.format((t2 - t1)))
    results = json.load(connection)
    results = results.get('response').get('docs')
    print('Total results {} from query'.format(len(results)))

    # Just loop over it to access the results.
    for result in results:
        print(result.get('image_link'), result.get('euclidean_distance'))


if __name__ == '__main__':
    connect()
