# Standard library
import concurrent.futures
from urllib.request import urlopen
import json
import time
import random


def connect(i):

    y = sample_floats(-2.00, 2.00, k = 128)
    print(f'--------------------- Started process {i} --------------------- ')

    v = ''
    for j in range(0,128):
        v = v + 'v{}_f'.format(str(j)) + ','

    url = 'http://127.0.0.1:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,' + v + \
    '{}'.format(','.join(str(element) for element in y)) + ')&fl=image_link,euclidean_distance:$euclidean_distance&\
sort=$euclidean_distance%20asc&rows=5'

    t1 = time.perf_counter()
    connection = urlopen(url)
    t2 = time.perf_counter()

    results = json.load(connection)
    results = results.get('response').get('docs')
    print('Completed process {}. Fetched {} result(s) from query in {} seconds!'.format(i, len(results), round(t2-t1, 2)))
    return t2-t1


def load_testing():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        avg_time = list()
        processes = int(input('Enter number of processes to spawn : '))
        start = time.perf_counter()
        results = [executor.submit(connect, i) for i in range(1, processes + 1)]
        for f in concurrent.futures.as_completed(results):
            avg_time.append(f.result())
        finish = time.perf_counter()
        print(f'\nAverage time for each request = {round(sum(avg_time) / len(avg_time), 2)} second(s)')
        print(f'Finished in {round(finish-start, 2)} second(s) using Multiprocessing\n')


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


if __name__ == '__main__':
    load_testing()
