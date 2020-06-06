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
        conn = psycopg2.connect(database = Credentials.database, user = Credentials.user, password = Credentials.password, 
        host = '127.0.0.1')

        # create a cursor
        cur = conn.cursor()
        cur.execute('''
        CREATE EXTENSION IF NOT EXISTS CUBE;
        CREATE TABLE IF NOT EXISTS PROJECT_WITH_CUBE(
            ID SERIAL PRIMARY KEY, 
            PROJECT_ID INTEGER NOT NULL, 
            IMAGE_LINK INTEGER NOT NULL, 
            VECTOR1 CUBE
        );
        CREATE INDEX IF NOT EXISTS VECTORS_VEC_IDX ON PROJECT_WITH_CUBE (VECTOR1);
        ''')
        conn.commit()

        x = y = list()
        
        y = sample_floats(-2.00, 2.00, k = 100)

        dist_programatically = list()
        sorted_list = list()

        cur.execute('''select count(*) from PROJECT_WITH_CUBE''')
        result = cur.fetchone()
        print(f'{result[0]} record(s) are present in table')

        records = int(input('\nEnter the number of records to insert : '))
        if records is None:
            records = 0

        # To update on previously added records
        cur.execute('''select max(id) from PROJECT_WITH_CUBE''')
        max_id = cur.fetchone()[0]
        restart = ''
        start = 1
        if max_id is not None:
            restart = input('\nResume insertion on previously inserted records? (y/n) : ')
            if restart == 'y':
                start = max_id + 1
                records += max_id  
            else: 
                cur.execute('''DELETE FROM PROJECT_WITH_CUBE''')
                conn.commit()
                print('\nDeleted previous records from table\n')

        for i in tqdm(range(start, records + 1)):
            x = sample_floats(-2.00, 2.00, k = 100)
            # Insert script

            query = "INSERT INTO project_with_cube (id, project_id, image_link, vector1) VALUES ({}, 1, {},\
            CUBE(array[{}]))".format(i, i, ','.join(str(element) for element in x))

            cur.execute(query)

            conn.commit()

        query = 'select vector1 from project_with_cube'
        cur.execute(query)
        arrays  = cur.fetchall()

        # Calculating programatically
        for array in arrays:
            x = list(map(float, (array[0])[1:-1].replace(' ', '').split(',')))
            dist = distance.euclidean(x, y)
            dist_programatically.append(round(dist, 2))

        dist_programatically = sorted(dist_programatically)
        sorted_list = dist_programatically[:5]

        cur.execute('''select count(*) from project_with_cube''')
        result = cur.fetchone()
        print(f'\n{result[0]} record(s) are present in database')

        query = " \
        SELECT  image_link, sqrt(power(CUBE(array[{}]) <-> vector1, 2)) as distance \
        FROM    project_with_cube \
        WHERE   PROJECT_ID = 1 \
        ORDER BY distance ASC LIMIT 5".format(','.join(str(element) for element in y))

        t1 = time.perf_counter()
        cur.execute(query)
        result = cur.fetchall()    
        t2 = time.perf_counter()

        print('\nTook {} seconds to calculate by query'.format(round(t2 - t1, 2)))

        print('\nResult calculated programatically = \n',sorted_list)

        print('\nResult from database = ')
        [print('Image: {}, Distance : {}'.format(res[0],res[1])) for res in result]

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except KeyboardInterrupt :
        print('\nExecution interrupted by user !!')
    finally:
        if conn is not None:
            conn.close()
            print('\nDatabase connection closed.\n')


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