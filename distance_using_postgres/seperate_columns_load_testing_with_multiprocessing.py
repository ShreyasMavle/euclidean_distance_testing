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

    print('Completed process {}. Fetched {} results from query !'.format(i, len(result)))

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
            print(f'Average time for each request = {round(sum(avg_time) / len(avg_time), 2)} second(s)')
            print(f'Finished in {round(finish-start, 2)} second(s) using Multiprocessing')
            

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
