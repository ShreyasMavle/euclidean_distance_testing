# Standard library
import random
from math import sqrt
import time

# Third party library
import psycopg2
from scipy.spatial import distance


# Internal imports
from config import config

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('\nConnecting to the PostgreSQL database...\n')
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        ######################################################################################
         # display the PostgreSQL database server version
        x = y = list()
        
        y = [-0.44, -1.25, 1.56, 0.98, -0.37, 1.19, -1.24, -0.22, -0.79, -0.12, 1.2, -1.93, -1.63, -1.87, -1.01, -1.29, -1.17, -1.33, -1.12, 0.79, 0.2, -0.45, 1.31, 1.21, 0.38, -0.76, -0.71, 0.42, -0.39, 0.39, 1.11, -1.46, 1.43, 1.76, 1.28, 1.62, 1.07, -1.66, 0.31, -1.53, 1.65, -0.88, 0.77, 1.31, -0.43, -0.69, -1.16, -0.56, -0.83, -0.17, -0.05, -0.47, 1.33, 1.37, 1.39, -1.45, 0.11, 1.43, 1.25, 0.52, -1.89, -0.41, -1.89, 1.93, -1.32, 1.18, 0.06, 1.38, -0.46, 0.41, -0.37, -0.86, 1.52, -0.84, -1.17, -1.8, 1.0, -0.15, -0.58, -1.18, 0.25, -0.15, -0.05, 0.94, -0.45, -0.19, 0.07, -1.63, 1.2, 1.25, 0.76, 1.51, 0.31, -1.34, -0.85, -0.82, -0.95, -0.12, -1.21, 1.0, -0.88, 1.58, -1.77, -1.45, 1.49, 0.36, -0.56, -0.54, 1.51, -0.29, 0.16, -0.48, -1.88, 1.76, -1.5, -1.86, -0.92, 0.6, -1.87, -0.65, 1.6, -1.8, -1.02, 0.87, 0.56, 0.29, 0.2, 1.47]

        print('y = ',y)
        print('Size of vector2 : ', len(y))
        
        dist_programatically = list()
        sorted_list = list()

        for i in range(1, 1001):
            x = sample_floats(-2.00, 2.00, k = 128)
            # Insert script
            cur.execute("INSERT INTO project VALUES (%s,%s,%s,%s)",(i,1,i,x))
            dist = distance.euclidean(x, y)
            dist_programatically.append(round(dist,2))
        
        conn.commit()

        for dist in sorted(dist_programatically):
            if len(sorted_list) < 5:
                sorted_list.append(dist)
            else:
                break

        cur.execute('''select count(vector1) from project''')
        result = cur.fetchone()
        print(f'{result[0]} records are present in database')

        t1 = time.perf_counter()
        cur.execute('''select * from dist_func(%s)''',(y,))
        result = cur.fetchall()
        t2 = time.perf_counter()
        print('Took {}s to calculate by query'.format((t2 - t1)))

        print('Result calculated programatically = ',sorted_list)
        print('Result from database = ', result) 

        cur.execute("DELETE FROM project")
        print('\nCleared table records')


        ######################################################################################
        conn.commit()
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
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
        result.append(round(x,2))
    return result



if __name__ == '__main__':
    connect()


