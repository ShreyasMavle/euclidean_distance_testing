# Standard library
import time
import multiprocessing
import random

# Third party library
import psycopg2


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
    conn = psycopg2.connect(database="euclidean", user="postgres",
                            password="Narrator@027", host='127.0.0.1')
    # create a cursor
    cur = conn.cursor()

    print(f'--------------------- Started process {i} --------------------- ')
    query = '''
        SELECT  image_link, sqrt(power(CUBE(array[{}]) <-> vector1, 2)) as distance
        FROM    project_with_cube
        WHERE   PROJECT_ID = 1
        ORDER BY distance ASC LIMIT 5'''.format(','.join(str(element) for element in y))

    t1 = time.perf_counter()
    cur.execute(query)
    result = cur.fetchall()
    t2 = time.perf_counter()

    execution_time = round((t2 - t1), 2)

    print('Completed process {}. Fetched {} results from query in {} seconds!'.format(i,
                                                                                      len(result), execution_time))

    if conn is not None:
        cur.close()
        conn.close()

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
