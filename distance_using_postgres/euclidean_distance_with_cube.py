# Standard library
import random
from math import sqrt
import time

# Third party library
import psycopg2
from scipy.spatial import distance
from tqdm import tqdm

# Internal imports
from database_credentials import Credentials

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('\nConnecting to the PostgreSQL database...\n')
        conn = psycopg2.connect(database = Credentials.database, user = Credentials.user, password = Credentials.password, host = '127.0.0.1')
        # create a cursor
        cur = conn.cursor()
        cur.execute('''
        CREATE EXTENSION IF NOT EXISTS CUBE;
        CREATE TABLE IF NOT EXISTS PROJECT_WITH_CUBE(
            ID SERIAL, 
            PROJECT_ID INTEGER, 
            IMAGE_LINK INTEGER, 
            VECTOR1 CUBE
        );
        CREATE INDEX IF NOT EXISTS VECTORS_VEC_IDX ON PROJECT_WITH_CUBE (VECTOR1);
        ''')
        conn.commit()

        x = y = list()
        
        y = [-0.44, -1.25, 1.56, 0.98, -0.37, 1.19, -1.24, -0.22, -0.79, -0.12, 1.2, -1.93, -1.63, -1.87, -1.01, -1.29, -1.17, 
        -1.33, -1.12, 0.79, 0.2, -0.45, 1.31, 1.21, 0.38, -0.76, -0.71, 0.42, -0.39, 0.39, 1.11, -1.46, 1.43, 1.76, 1.28, 1.62, 
        1.07, -1.66, 0.31, -1.53, 1.65, -0.88, 0.77, 1.31, -0.43, -0.69, -1.16, -0.56, -0.83, -0.17, -0.05, -0.47, 1.33, 1.37, 
        1.39, -1.45, 0.11, 1.43, 1.25, 0.52, -1.89, -0.41, -1.89, 1.93, -1.32, 1.18, 0.06, 1.38, -0.46, 0.41, -0.37, -0.86, 
        1.52, -0.84, -1.17, -1.8, 1.0, -0.15, -0.58, -1.18, 0.25, -0.15, -0.05, 0.94, -0.45, -0.19, 0.07, -1.63, 1.2, 1.25, 
        0.76, 1.51, 0.31, -1.34, -0.85, -0.82, -0.95, -0.12, -1.21, 1.0, -0.88, 1.58, -1.77, -1.45, 1.49, 0.36, -0.56, -0.54, 
        1.51, -0.29, 0.16, -0.48, -1.88, 1.76, -1.5, -1.86, -0.92, 0.6, -1.87, -0.65, 1.6, -1.8, -1.02, 0.87, 0.56, 0.29, 0.2, 
        1.47]

        dist_programatically = list()
        sorted_list = list()

        cur.execute('''select count(*) from PROJECT_WITH_CUBE''')
        result = cur.fetchone()
        print(f'{result[0]} record(s) are present in table')

        records = int(input('Enter the number of records to insert : '))
        if records is None:
            records = 0

        # To update on previously added records
        cur.execute('''select max(id) from PROJECT_WITH_CUBE''')
        max_id = cur.fetchone()[0]
        restart = ''
        start = 1
        if max_id is not None:
            restart = input('Resume insertion on previously inserted records? (y/n) ')
            if restart == 'y':
                start = max_id + 1
                records += max_id  
            else: 
                cur.execute('''DELETE FROM PROJECT_WITH_CUBE''')
                conn.commit()
                print('Deleted previous records from table')

        for i in tqdm(range(start, records + 1)):
            x = sample_floats(-2.00, 2.00, k = 128)
            # Insert script

            query = "INSERT INTO project_with_cube (id, project_id, image_link, vector1) VALUES ({}, 1, {},\
            CUBE(array[{}]))".format(i, i, ','.join(str(element) for element in x))

            cur.execute(query)

            dist = distance.euclidean(x, y)
            dist_programatically.append(round(dist,2))

            conn.commit()

        dist_programatically = sorted(dist_programatically)
        sorted_list = dist_programatically[:5]

        cur.execute('''select count(*) from project_with_cube''')
        result = cur.fetchone()
        print(f'\n{result[0]} record(s) are present in database')

        
        query = "SELECT image_link, sqrt(power(CUBE(array[{}]) <-> vector1, 2)) as distance FROM project_with_cube ORDER BY\
             distance ASC LIMIT 5".format(','.join(str(element) for element in y))


        t1 = time.perf_counter()
        cur.execute(query)
        result = cur.fetchall()    
        t2 = time.perf_counter()

        print('Took {}s to calculate by query'.format((t2 - t1)))

        print('Result calculated programatically = \n',sorted_list)

        print('Result from database = ')
        [print('Image: {}, Distance : {}'.format(res[0],res[1])) for res in result]

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
        result.append(round(x,2))
    return result



if __name__ == '__main__':
    connect()