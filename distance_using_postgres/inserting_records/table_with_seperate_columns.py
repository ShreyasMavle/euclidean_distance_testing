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
        conn = psycopg2.connect(database = Credentials.database, user = Credentials.user, 
                                password = Credentials.password, host = '127.0.0.1')
        # create a cursor
        cur = conn.cursor()

        
        cur.execute('''
        CREATE TABLE IF NOT EXISTS project_with_seperate_columns (
            id serial PRIMARY KEY,
            project_id integer NOT NULL,
            image_link integer NOT NULL,
            x1 		float 	NOT NULL,
            x2 		float 	NOT NULL,
            x3	    float	NOT NULL,
            x4	    float	NOT NULL,
            x5	    float	NOT NULL,
            x6	    float	NOT NULL,
            x7	    float	NOT NULL,
            x8	    float	NOT NULL,
            x9	    float	NOT NULL,
            x10	    float	NOT NULL,
            x11	    float	NOT NULL,
            x12	    float	NOT NULL,
            x13	    float	NOT NULL,
            x14	    float	NOT NULL,
            x15	    float	NOT NULL,
            x16	    float	NOT NULL,
            x17	    float	NOT NULL,
            x18	    float	NOT NULL,
            x19	    float	NOT NULL,
            x20	    float	NOT NULL,
            x21	    float	NOT NULL,
            x22	    float	NOT NULL,
            x23	    float	NOT NULL,
            x24	    float	NOT NULL,
            x25	    float	NOT NULL,
            x26	    float	NOT NULL,
            x27	    float	NOT NULL,
            x28	    float	NOT NULL,
            x29	    float	NOT NULL,
            x30	    float	NOT NULL,
            x31	    float	NOT NULL,
            x32	    float	NOT NULL,
            x33	    float	NOT NULL,
            x34	    float	NOT NULL,
            x35	    float	NOT NULL,
            x36	    float	NOT NULL,
            x37	    float	NOT NULL,
            x38	    float	NOT NULL,
            x39	    float	NOT NULL,
            x40	    float	NOT NULL,
            x41	    float	NOT NULL,
            x42	    float	NOT NULL,
            x43	    float	NOT NULL,
            x44	    float	NOT NULL,
            x45	    float	NOT NULL,
            x46	    float	NOT NULL,
            x47	    float	NOT NULL,
            x48	    float	NOT NULL,
            x49	    float	NOT NULL,
            x50	    float	NOT NULL,
            x51	    float	NOT NULL,
            x52	    float	NOT NULL,
            x53	    float	NOT NULL,
            x54	    float	NOT NULL,
            x55	    float	NOT NULL,
            x56	    float	NOT NULL,
            x57	    float	NOT NULL,
            x58	    float	NOT NULL,
            x59	    float	NOT NULL,
            x60	    float	NOT NULL,
            x61	    float	NOT NULL,
            x62	    float	NOT NULL,
            x63	    float	NOT NULL,
            x64	    float	NOT NULL,
            x65	    float	NOT NULL,
            x66	    float	NOT NULL,
            x67	    float	NOT NULL,
            x68	    float	NOT NULL,
            x69	    float	NOT NULL,
            x70	    float	NOT NULL,
            x71	    float	NOT NULL,
            x72	    float	NOT NULL,
            x73	    float	NOT NULL,
            x74	    float	NOT NULL,
            x75	    float	NOT NULL,
            x76	    float	NOT NULL,
            x77	    float	NOT NULL,
            x78	    float	NOT NULL,
            x79	    float	NOT NULL,
            x80	    float	NOT NULL,
            x81	    float	NOT NULL,
            x82	    float	NOT NULL,
            x83	    float	NOT NULL,
            x84	    float	NOT NULL,
            x85	    float	NOT NULL,
            x86	    float	NOT NULL,
            x87	    float	NOT NULL,
            x88	    float	NOT NULL,
            x89	    float	NOT NULL,
            x90	    float	NOT NULL,
            x91	    float	NOT NULL,
            x92	    float	NOT NULL,
            x93	    float	NOT NULL,
            x94	    float	NOT NULL,
            x95	    float	NOT NULL,
            x96	    float	NOT NULL,
            x97	    float	NOT NULL,
            x98	    float	NOT NULL,
            x99	    float	NOT NULL,
            x100	float	NOT NULL,
            x101	float	NOT NULL,
            x102	float	NOT NULL,
            x103	float	NOT NULL,
            x104	float	NOT NULL,
            x105	float	NOT NULL,
            x106	float	NOT NULL,
            x107	float	NOT NULL,
            x108	float	NOT NULL,
            x109	float	NOT NULL,
            x110	float	NOT NULL,
            x111	float	NOT NULL,
            x112	float	NOT NULL,
            x113	float	NOT NULL,
            x114	float	NOT NULL,
            x115	float	NOT NULL,
            x116	float	NOT NULL,
            x117	float	NOT NULL,
            x118	float	NOT NULL,
            x119	float	NOT NULL,
            x120	float	NOT NULL,
            x121	float	NOT NULL,
            x122	float	NOT NULL,
            x123	float	NOT NULL,
            x124	float	NOT NULL,
            x125	float	NOT NULL,
            x126	float	NOT NULL,
            x127	float	NOT NULL,
            x128	float	NOT NULL);
        ''')

        conn.commit()

        y = list()
        
        y = sample_floats(-2.00, 2.00, k = 128)
        
        dist_programatically = list()
        sorted_list = list()

        cur.execute('''select count(*) from project_with_seperate_columns''')
        result = cur.fetchone()
        print(f'{result[0]} record(s) are present in table')

        records = int(input('\nEnter the number of records to insert : '))
        if records is None:
            records = 0

        # To update on previously added records
        cur.execute('''select max(id) from project_with_seperate_columns''')
        max_id = cur.fetchone()[0]
        restart = ''
        start = 1
        if max_id is not None:
            restart = input('Resume insertion on previously inserted records? (y/n) : ')
            if restart == 'y':
                start = max_id + 1
                records += max_id  
            else: 
                cur.execute('''DELETE FROM project_with_seperate_columns''')
                conn.commit()
                print('\nDeleted previous records from table')

        for i in tqdm(range(start, records + 1)):
            x = sample_floats(-2.00, 2.00, k = 128)

            # Insert script
            cur.execute("INSERT INTO project_with_seperate_columns VALUES (%s,%s,%s,\
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(i,1,i,
            x[0],  x[1],  x[2],  x[3],  x[4],  x[5],  x[6],  x[7],  x[8],  x[9], 
            x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], 
            x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29], 
            x[30], x[31], x[32], x[33], x[34], x[35], x[36], x[37], x[38], x[39], 
            x[40], x[41], x[42], x[43], x[44], x[45], x[46], x[47], x[48], x[49],
            x[50], x[51], x[52], x[53], x[54], x[55], x[56], x[57], x[58], x[59], 
            x[60], x[61], x[62], x[63], x[64], x[65], x[66], x[67], x[68], x[69], 
            x[70], x[71], x[72], x[73], x[74], x[75], x[76], x[77], x[78], x[79], 
            x[80], x[81], x[82], x[83], x[84], x[85], x[86], x[87], x[88], x[89], 
            x[90], x[91], x[92], x[93], x[94], x[95], x[96], x[97], x[98], x[99], 
            x[100], x[101], x[102], x[103], x[104], x[105], x[106], x[107], x[108], 
            x[109], x[110], x[111], x[112], x[113], x[114], x[115], x[116], x[117], 
            x[118], x[119], x[120], x[121], x[122], x[123], x[124], x[125], x[126], 
            x[127]))

        conn.commit()

        x_list = (f'x{x}' for x in range(1,129))

        query = 'select ' + ','.join(x_list) + ' from project_with_seperate_columns'
        cur.execute(query)
        arrays  = cur.fetchall()

        # Calculating programatically
        for array in arrays:
            dist = distance.euclidean(list(array), y)
            dist_programatically.append(round(dist, 2))

        dist_programatically = sorted(dist_programatically)
        sorted_list = dist_programatically[:5]
           
        cur.execute('''select count(*) from project_with_seperate_columns''')
        result = cur.fetchone()
        print(f'\n{result[0]} record(s) are present in table')

        t1 = time.perf_counter()
        cur.execute('''
        SELECT
            p.image_link as image, sqrt(
            power(x1   -  %s,2) + 
            power(x2   -  %s,2) +
            power(x3   -  %s,2) +
            power(x4   -  %s,2) +
            power(x5   -  %s,2) +
            power(x6   -  %s,2) +
            power(x7   -  %s,2) +
            power(x8   -  %s,2) +
            power(x9   -  %s,2) +
            power(x10  -  %s,2) +
            power(x11  -  %s,2) +
            power(x12  -  %s,2) +
            power(x13  -  %s,2) +
            power(x14  -  %s,2) +
            power(x15  -  %s,2) +
            power(x16  -  %s,2) +
            power(x17  -  %s,2) +
            power(x18  -  %s,2) +
            power(x19  -  %s,2) +
            power(x20  -  %s,2) +
            power(x21  -  %s,2) +
            power(x22  -  %s,2) +
            power(x23  -  %s,2) +
            power(x24  -  %s,2) +
            power(x25  -  %s,2) +
            power(x26  -  %s,2) +
            power(x27  -  %s,2) +
            power(x28  -  %s,2) +
            power(x29  -  %s,2) +
            power(x30  -  %s,2) +
            power(x31  -  %s,2) +
            power(x32  -  %s,2) +
            power(x33  -  %s,2) +
            power(x34  -  %s,2) +
            power(x35  -  %s,2) +
            power(x36  -  %s,2) +
            power(x37  -  %s,2) +
            power(x38  -  %s,2) +
            power(x39  -  %s,2) +
            power(x40  -  %s,2) +
            power(x41  -  %s,2) +
            power(x42  -  %s,2) +
            power(x43  -  %s,2) +
            power(x44  -  %s,2) +
            power(x45  -  %s,2) +
            power(x46  -  %s,2) +
            power(x47  -  %s,2) +
            power(x48  -  %s,2) +
            power(x49  -  %s,2) +
            power(x50  -  %s,2) +
            power(x51  -  %s,2) +
            power(x52  -  %s,2) +
            power(x53  -  %s,2) +
            power(x54  -  %s,2) +
            power(x55  -  %s,2) +
            power(x56  -  %s,2) +
            power(x57  -  %s,2) +
            power(x58  -  %s,2) +
            power(x59  -  %s,2) +
            power(x60  -  %s,2) +
            power(x61  -  %s,2) +
            power(x62  -  %s,2) +
            power(x63  -  %s,2) +
            power(x64  -  %s,2) +
            power(x65  -  %s,2) +
            power(x66  -  %s,2) +
            power(x67  -  %s,2) +
            power(x68  -  %s,2) +
            power(x69  -  %s,2) +
            power(x70  -  %s,2) +
            power(x71  -  %s,2) +
            power(x72  -  %s,2) +
            power(x73  -  %s,2) +
            power(x74  -  %s,2) +
            power(x75  -  %s,2) +
            power(x76  -  %s,2) +
            power(x77  -  %s,2) +
            power(x78  -  %s,2) +
            power(x79  -  %s,2) +
            power(x80  -  %s,2) +
            power(x81  -  %s,2) +
            power(x82  -  %s,2) +
            power(x83  -  %s,2) +
            power(x84  -  %s,2) +
            power(x85  -  %s,2) +
            power(x86  -  %s,2) +
            power(x87  -  %s,2) +
            power(x88  -  %s,2) +
            power(x89  -  %s,2) +
            power(x90  -  %s,2) +
            power(x91  -  %s,2) +
            power(x92  -  %s,2) +
            power(x93  -  %s,2) +
            power(x94  -  %s,2) +
            power(x95  -  %s,2) +
            power(x96  -  %s,2) +
            power(x97  -  %s,2) +
            power(x98  -  %s,2) +
            power(x99  -  %s,2) +
            power(x100 -  %s,2) +
            power(x101 -  %s,2) +
            power(x102 -  %s,2) +
            power(x103 -  %s,2) +
            power(x104 -  %s,2) +
            power(x105 -  %s,2) +
            power(x106 -  %s,2) +
            power(x107 -  %s,2) +
            power(x108 -  %s,2) +
            power(x109 -  %s,2) +
            power(x110 -  %s,2) +
            power(x111 -  %s,2) +
            power(x112 -  %s,2) +
            power(x113 -  %s,2) +
            power(x114 -  %s,2) +
            power(x115 -  %s,2) +
            power(x116 -  %s,2) +
            power(x117 -  %s,2) +
            power(x118 -  %s,2) +
            power(x119 -  %s,2) +
            power(x120 -  %s,2) +
            power(x121 -  %s,2) +
            power(x122 -  %s,2) +
            power(x123 -  %s,2) +
            power(x124 -  %s,2) +
            power(x125 -  %s,2) +
            power(x126 -  %s,2) +
            power(x127 -  %s,2) +
            power(x128 -  %s,2))
            as distances
        FROM
            project_with_seperate_columns p
        WHERE   
            PROJECT_ID = 1
        order by 
            distances asc
        limit 5''',(
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
        print('\nTook {} seconds to calculate by query'.format(round(t2 - t1, 2)))

        print('\nResult calculated programatically = \n',sorted_list)

        print('\nResult from database = ')
        [print('Image: {}, Distance : {}'.format(res[0],res[1])) for res in result]

	    # close the communication with the PostgreSQL
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


