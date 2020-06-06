# Standard library
import random
from math import sqrt
import time

# Third party library
import psycopg2
from scipy.spatial import distance

# Internal imports
from database_credentials import Credentials

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('\nConnecting to the PostgreSQL database...\n')
        conn = psycopg2.connect(database = Credentials.database, user = Credentials.user, password = Credentials.password)
        # create a cursor
        cur = conn.cursor()

        cur.execute('''
        CREATE TABLE IF NOT EXISTS project_with_vectors (
            id serial PRIMARY KEY,
            project_id integer NOT NULL,
            image_link integer NOT NULL, 	-- need to put a unique not null constraint
            vector1 float[] NOT NULL
        );
        ''')

        x = y = list()
        
        y = [-0.44, -1.25, 1.56, 0.98, -0.37, 1.19, -1.24, -0.22, -0.79, -0.12, 1.2, -1.93, -1.63, -1.87, -1.01, -1.29, -1.17, -1.33, -1.12, 0.79, 0.2, -0.45, 1.31, 1.21, 0.38, -0.76, -0.71, 0.42, -0.39, 0.39, 1.11, -1.46, 1.43, 1.76, 1.28, 1.62, 1.07, -1.66, 0.31, -1.53, 1.65, -0.88, 0.77, 1.31, -0.43, -0.69, -1.16, -0.56, -0.83, -0.17, -0.05, -0.47, 1.33, 1.37, 1.39, -1.45, 0.11, 1.43, 1.25, 0.52, -1.89, -0.41, -1.89, 1.93, -1.32, 1.18, 0.06, 1.38, -0.46, 0.41, -0.37, -0.86, 1.52, -0.84, -1.17, -1.8, 1.0, -0.15, -0.58, -1.18, 0.25, -0.15, -0.05, 0.94, -0.45, -0.19, 0.07, -1.63, 1.2, 1.25, 0.76, 1.51, 0.31, -1.34, -0.85, -0.82, -0.95, -0.12, -1.21, 1.0, -0.88, 1.58, -1.77, -1.45, 1.49, 0.36, -0.56, -0.54, 1.51, -0.29, 0.16, -0.48, -1.88, 1.76, -1.5, -1.86, -0.92, 0.6, -1.87, -0.65, 1.6, -1.8, -1.02, 0.87, 0.56, 0.29, 0.2, 1.47]
   
        dist_programatically = list()
        sorted_list = list()

        records = int(input('Enter the number of records to insert : '))

        for i in range(1, records + 1):
            x = sample_floats(-2.00, 2.00, k = 128)
            # Insert script
            cur.execute("INSERT INTO project_with_vectors VALUES (%s,%s,%s,%s)",(i,1,i,x))
            print('Inserted {} records !!'.format(i))
            
            dist = distance.euclidean(x, y)
            dist_programatically.append(round(dist,2))
        
        conn.commit()

        dist_programatically = sorted(dist_programatically)
        sorted_list = dist_programatically[:6]

        cur.execute('''select count(vector1) from project_with_vectors''')
        result = cur.fetchone()
        print(f'{result[0]} records are present in database')

        t1 = time.perf_counter()
        cur.execute('''
        SELECT
            p.image_link as image, sqrt(
            power(p.vector1[1]   -  %s,2) + 
            power(p.vector1[2]   -  %s,2) +
            power(p.vector1[3]   -  %s,2) +
            power(p.vector1[4]   -  %s,2) +
            power(p.vector1[5]   -  %s,2) +
            power(p.vector1[6]   -  %s,2) +
            power(p.vector1[7]   -  %s,2) +
            power(p.vector1[8]   -  %s,2) +
            power(p.vector1[9]   -  %s,2) +
            power(p.vector1[10]  -  %s,2) +
            power(p.vector1[11]  -  %s,2) +
            power(p.vector1[12]  -  %s,2) +
            power(p.vector1[13]  -  %s,2) +
            power(p.vector1[14]  -  %s,2) +
            power(p.vector1[15]  -  %s,2) +
            power(p.vector1[16]  -  %s,2) +
            power(p.vector1[17]  -  %s,2) +
            power(p.vector1[18]  -  %s,2) +
            power(p.vector1[19]  -  %s,2) +
            power(p.vector1[20]  -  %s,2) +
            power(p.vector1[21]  -  %s,2) +
            power(p.vector1[22]  -  %s,2) +
            power(p.vector1[23]  -  %s,2) +
            power(p.vector1[24]  -  %s,2) +
            power(p.vector1[25]  -  %s,2) +
            power(p.vector1[26]  -  %s,2) +
            power(p.vector1[27]  -  %s,2) +
            power(p.vector1[28]  -  %s,2) +
            power(p.vector1[29]  -  %s,2) +
            power(p.vector1[30]  -  %s,2) +
            power(p.vector1[31]  -  %s,2) +
            power(p.vector1[32]  -  %s,2) +
            power(p.vector1[33]  -  %s,2) +
            power(p.vector1[34]  -  %s,2) +
            power(p.vector1[35]  -  %s,2) +
            power(p.vector1[36]  -  %s,2) +
            power(p.vector1[37]  -  %s,2) +
            power(p.vector1[38]  -  %s,2) +
            power(p.vector1[39]  -  %s,2) +
            power(p.vector1[40]  -  %s,2) +
            power(p.vector1[41]  -  %s,2) +
            power(p.vector1[42]  -  %s,2) +
            power(p.vector1[43]  -  %s,2) +
            power(p.vector1[44]  -  %s,2) +
            power(p.vector1[45]  -  %s,2) +
            power(p.vector1[46]  -  %s,2) +
            power(p.vector1[47]  -  %s,2) +
            power(p.vector1[48]  -  %s,2) +
            power(p.vector1[49]  -  %s,2) +
            power(p.vector1[50]  -  %s,2) +
            power(p.vector1[51]  -  %s,2) +
            power(p.vector1[52]  -  %s,2) +
            power(p.vector1[53]  -  %s,2) +
            power(p.vector1[54]  -  %s,2) +
            power(p.vector1[55]  -  %s,2) +
            power(p.vector1[56]  -  %s,2) +
            power(p.vector1[57]  -  %s,2) +
            power(p.vector1[58]  -  %s,2) +
            power(p.vector1[59]  -  %s,2) +
            power(p.vector1[60]  -  %s,2) +
            power(p.vector1[61]  -  %s,2) +
            power(p.vector1[62]  -  %s,2) +
            power(p.vector1[63]  -  %s,2) +
            power(p.vector1[64]  -  %s,2) +
            power(p.vector1[65]  -  %s,2) +
            power(p.vector1[66]  -  %s,2) +
            power(p.vector1[67]  -  %s,2) +
            power(p.vector1[68]  -  %s,2) +
            power(p.vector1[69]  -  %s,2) +
            power(p.vector1[70]  -  %s,2) +
            power(p.vector1[71]  -  %s,2) +
            power(p.vector1[72]  -  %s,2) +
            power(p.vector1[73]  -  %s,2) +
            power(p.vector1[74]  -  %s,2) +
            power(p.vector1[75]  -  %s,2) +
            power(p.vector1[76]  -  %s,2) +
            power(p.vector1[77]  -  %s,2) +
            power(p.vector1[78]  -  %s,2) +
            power(p.vector1[79]  -  %s,2) +
            power(p.vector1[80]  -  %s,2) +
            power(p.vector1[81]  -  %s,2) +
            power(p.vector1[82]  -  %s,2) +
            power(p.vector1[83]  -  %s,2) +
            power(p.vector1[84]  -  %s,2) +
            power(p.vector1[85]  -  %s,2) +
            power(p.vector1[86]  -  %s,2) +
            power(p.vector1[87]  -  %s,2) +
            power(p.vector1[88]  -  %s,2) +
            power(p.vector1[89]  -  %s,2) +
            power(p.vector1[90]  -  %s,2) +
            power(p.vector1[91]  -  %s,2) +
            power(p.vector1[92]  -  %s,2) +
            power(p.vector1[93]  -  %s,2) +
            power(p.vector1[94]  -  %s,2) +
            power(p.vector1[95]  -  %s,2) +
            power(p.vector1[96]  -  %s,2) +
            power(p.vector1[97]  -  %s,2) +
            power(p.vector1[98]  -  %s,2) +
            power(p.vector1[99]  -  %s,2) +
            power(p.vector1[100] -  %s,2) +
            power(p.vector1[101] -  %s,2) +
            power(p.vector1[102] -  %s,2) +
            power(p.vector1[103] -  %s,2) +
            power(p.vector1[104] -  %s,2) +
            power(p.vector1[105] -  %s,2) +
            power(p.vector1[106] -  %s,2) +
            power(p.vector1[107] -  %s,2) +
            power(p.vector1[108] -  %s,2) +
            power(p.vector1[109] -  %s,2) +
            power(p.vector1[110] -  %s,2) +
            power(p.vector1[111] -  %s,2) +
            power(p.vector1[112] -  %s,2) +
            power(p.vector1[113] -  %s,2) +
            power(p.vector1[114] -  %s,2) +
            power(p.vector1[115] -  %s,2) +
            power(p.vector1[116] -  %s,2) +
            power(p.vector1[117] -  %s,2) +
            power(p.vector1[118] -  %s,2) +
            power(p.vector1[119] -  %s,2) +
            power(p.vector1[120] -  %s,2) +
            power(p.vector1[121] -  %s,2) +
            power(p.vector1[122] -  %s,2) +
            power(p.vector1[123] -  %s,2) +
            power(p.vector1[124] -  %s,2) +
            power(p.vector1[125] -  %s,2) +
            power(p.vector1[126] -  %s,2) +
            power(p.vector1[127] -  %s,2) +
            power(p.vector1[128] -  %s,2))
            as distances
        FROM
            project_with_vectors p
        order by 
            distances asc
        limit 5;''',(
        y[0],  y[1],  y[2],  y[3],  y[4],  y[5],  y[6],  y[7],  y[8],  y[9], y[10], y[11], y[12], y[13], y[14], y[15], y[16], 
        y[17], y[18], y[19], y[20], y[21], y[22], y[23], y[24], y[25], y[26], y[27], y[28], y[29], y[30], y[31], y[32], y[33], 
        y[34], y[35], y[36], y[37], y[38], y[39], y[40], y[41], y[42], y[43], y[44], y[45], y[46], y[47], y[48], y[49],y[50], 
        y[51], y[52], y[53], y[54], y[55], y[56], y[57], y[58], y[59], y[60], y[61], y[62], y[63], y[64], y[65], y[66], y[67], 
        y[68], y[69], y[70], y[71], y[72], y[73], y[74], y[75], y[76], y[77], y[78], y[79], y[80], y[81], y[82], y[83], y[84], 
        y[85], y[86], y[87], y[88], y[89], y[90], y[91], y[92], y[93], y[94], y[95], y[96], y[97], y[98], y[99], y[100], y[101], 
        y[102], y[103], y[104], y[105], y[106], y[107], y[108], y[109], y[110], y[111], y[112], y[113], y[114], y[115], y[116], 
        y[117], y[118], y[119], y[120], y[121], y[122], y[123], y[124], y[125], y[126], y[127]))

        result = cur.fetchall()
        t2 = time.perf_counter()
        print('Took {}s to calculate by query'.format((t2 - t1)))

        print('Result calculated programatically = \n',sorted_list)

        print('Result from database = ')
        [print('Image: {}, Distance : {}'.format(res[0],res[1])) for res in result]

        # cur.execute('''DELETE FROM project_with_vectors''')
        # conn.commit()

	    # close the communication with the PostgreSQL
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


