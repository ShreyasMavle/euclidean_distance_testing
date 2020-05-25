# Standard library
import concurrent.futures
import json
import time
import random

# Third party library
import psycopg2

# Internal imports
from database_credentials import Credentials


def connect(i):

    y = sample_floats(-2.00, 2.00, k=128)
    conn = psycopg2.connect(database=Credentials.database, user=Credentials.user, password=Credentials.password)
    # create a cursor
    cur = conn.cursor()

    print(f'--------------------- Started process {i} --------------------- ')
    query = "SELECT image_link, sqrt(power(CUBE(array[{}]) <-> vector1, 2)) as distance FROM project_with_cube ORDER BY\
             distance ASC LIMIT 5".format(','.join(str(element) for element in y))

    t1 = time.perf_counter()
    cur.execute(query)
    result = cur.fetchall()
    t2 = time.perf_counter()

    print('Completed process {}. Fetched {} results from query in {} seconds!'.format(i, len(result), (t2-t1)))

    if conn is not None:
        cur.close()
        conn.close()
    return t2-t1


def load_testing():
    try:
        processes = int(input('Enter number of processes to spawn : '))

        with concurrent.futures.ProcessPoolExecutor() as executor:
            avg_time = list()
            start = time.perf_counter()
            results = [executor.submit(connect, i) for i in range(processes + 1)]

            for f in concurrent.futures.as_completed(results):
                avg_time.append(f.result())
                
            finish = time.perf_counter()
            print(
                f'Average time for each request = {round(sum(avg_time) / len(avg_time), 2)} second(s)')
            print(
                f'Finished in {round(finish-start, 2)} second(s) using Multiprocessing')
            

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except KeyboardInterrupt :
        print('\nExecution interrupted by user !!')
    


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


if __name__ == '__main__':
    load_testing()