# Standard library
from urllib.request import urlopen
import json
import time
import random
import multiprocessing


def sample_floats(low, high, k=1):
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


def connect(i, queue):

    y = sample_floats(-2.00, 2.00, k=128)
    print(f'--------------------- Started process {i} --------------------- ')

    v = ''
    for j in range(0, 128):
        v = v + 'v{}_f'.format(str(j)) + ','

    url = 'http://127.0.0.1:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,' + v + \
        '{}'.format(','.join(str(element) for element in y)) + ')&fl=image_link,euclidean_distance:$euclidean_distance&\
sort=$euclidean_distance%20asc&rows=5'

    t1 = time.perf_counter()
    connection = urlopen(url)
    t2 = time.perf_counter()

    execution_time = round((t2 - t1), 2)

    results = json.load(connection)
    results = results.get('response').get('docs')
    print('Completed process {}. Fetched {} result(s) from query in {} seconds!'.format(
        i, len(results), execution_time))

    queue.put(execution_time)


if __name__ == '__main__':
    avg_time = list()
    processes = []
    input_processes = int(input('Enter number of processes to spawn : '))
    queue = multiprocessing.Queue()
    start_time = time.time()

    for i in range(input_processes + 1):
        process = multiprocessing.Process(target=connect, args=(i, queue))
        processes.append(process)
        process.start()

    for process in processes:
        avg_time.append(queue.get())
        process.join()

    finish_time = time.time()

    print(
        f"Average time for each request = {round(sum(avg_time) / len(avg_time), 2)} second(s)")

    print(
        f"Finished in {round((finish_time - start_time), 2)} second(s) using Multiprocessing")
