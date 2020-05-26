# Standard library
import time
import random

# Third party library
import psycopg2

# Internal imports
from database_credentials import Credentials

def load_testing():
    try:
        print('\nConnecting to the PostgreSQL database...\n')
        conn = psycopg2.connect(database=Credentials.database, user=Credentials.user, password=Credentials.password, host = '127.0.0.1')

        # create a cursor
        cur = conn.cursor()
        iterations = int(input('Enter number of times to run query : '))

        avg_time = list()

        for i in range(iterations + 1):

            y = sample_floats(-2.00, 2.00, k = 128)
            query = "SELECT image_link, sqrt(power(CUBE(array[{}]) <-> vector1, 2)) as distance FROM project_with_cube ORDER BY\
                distance ASC LIMIT 5".format(','.join(str(element) for element in y))

            t1 = time.perf_counter()
            cur.execute(query)
            result = cur.fetchall()
            t2 = time.perf_counter()

            print('Completed iteration {}. Fetched {} results from query in {}s!'.format(i, len(result), (t2-t1)))
            avg_time.append(t2-t1)
                    

        print(f'Average time taken by each query = {round(sum(avg_time) / len(avg_time), 2)} second(s)')
   
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except KeyboardInterrupt :
        print('\nExecution interrupted by user !!')
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


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
