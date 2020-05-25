import concurrent.futures
from urllib.request import urlopen
import json
import time
import random


def connect(i, y):
    t1 = time.perf_counter()
    print(f'--------------------- Started process {i} --------------------- ')
    url = 'http://localhost:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,v0_f,v1_f,v2_f,v3_f,v4_f,v5_f,v6_f,\
v7_f,v8_f,v9_f,v10_f,v11_f,v12_f,v13_f,v14_f,v15_f,v16_f,v17_f,v18_f,v19_f,v20_f,v21_f,v22_f,v23_f,v24_f,v25_f,v26_f,v27_f,v28_f\
,v29_f,v30_f,v31_f,v32_f,v33_f,v34_f,v35_f,v36_f,v37_f,v38_f,v39_f,v40_f,v41_f,v42_f,v43_f,v44_f,v45_f,v46_f,v47_f,v48_f,v49_f,\
v50_f,v51_f,v52_f,v53_f,v54_f,v55_f,v56_f,v57_f,v58_f,v59_f,v60_f,v61_f,v62_f,v63_f,v64_f,v65_f,v66_f,v67_f,v68_f,v69_f,v70_f,\
v71_f,v72_f,v73_f,v74_f,v75_f,v76_f,v77_f,v78_f,v79_f,v80_f,v81_f,v82_f,v83_f,v84_f,v85_f,v86_f,v87_f,v88_f,v89_f,v90_f,v91_f,\
v92_f,v93_f,v94_f,v95_f,v96_f,v97_f,v98_f,v99_f,v100_f,v101_f,v102_f,v103_f,v104_f,v105_f,v106_f,v107_f,v108_f,v109_f,v110_f,\
v111_f,v112_f,v113_f,v114_f,v115_f,v116_f,v117_f,v118_f,v119_f,v120_f,v121_f,v122_f,v123_f,v124_f,v125_f,v126_f,v127_f,-0.44,\
-1.25,1.56,0.98,-0.37,1.19,-1.24,-0.22,-0.79,-0.12,1.2,-1.93,-1.63,-1.87,-1.01,-1.29,-1.17,-1.33,-1.12,0.79,0.2,-0.45,1.31,1.21\
,0.38,-0.76,-0.71,0.42,-0.39,0.39,1.11,-1.46,1.43,1.76,1.28,1.62,1.07,-1.66,0.31,-1.53,1.65,-0.88,0.77,1.31,-0.43,-0.69,-1.16,\
-0.56,-0.83,-0.17,-0.05,-0.47,1.33,1.37,1.39,-1.45,0.11,1.43,1.25,0.52,-1.89,-0.41,-1.89,1.93,-1.32,1.18,0.06,1.38,-0.46,0.41,\
-0.37,-0.86,1.52,-0.84,-1.17,-1.8,1.0,-0.15,-0.58,-1.18,0.25,-0.15,-0.05,0.94,-0.45,-0.19,0.07,-1.63,1.2,1.25,0.76,1.51,0.31,\
-1.34,-0.85,-0.82,-0.95,-0.12,-1.21,1.0,-0.88,1.58,-1.77,-1.45,1.49,0.36,-0.56,-0.54,1.51,-0.29,0.16,-0.48,-1.88,1.76,-1.5,\
-1.86,-0.92,0.6,-1.87,-0.65,1.6,-1.8,-1.02,{},{},{},{},{})&fl=image_link,euclidean_distance:$euclidean_distance&\
sort=$euclidean_distance%20asc&rows=5'.format(y[0], y[1], y[2], y[3], y[4])
    connection = urlopen(url)
    t2 = time.perf_counter()
    results = json.load(connection)
    results = results.get('response').get('docs')
    print('Completed process {}. Fetched {} results from query !'.format(i, len(results)))
    return t2-t1


def load_testing():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        avg_time = list()
        threads = int(input('Enter number of threads to spawn : '))
        start = time.perf_counter()
        results = [executor.submit(
            connect, i, sample_floats(-2.00, 2.00, k=5)) for i in range(threads + 1)]
        for f in concurrent.futures.as_completed(results):
            avg_time.append(f.result())
        finish = time.perf_counter()
        print(
            f'Average time for each request = {round(sum(avg_time) / len(avg_time), 2)} second(s)')
        print(f'Finished in {round(finish-start, 2)} second(s) using Threading')


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
