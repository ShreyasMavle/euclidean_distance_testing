# Standard library
from urllib.request import urlopen
import random
import json
import time

# Third party library
import pysolr
from scipy.spatial import distance
from tqdm import tqdm


def connect():
    # Setup a Solr instance. The timeout is optional.
    solr = pysolr.Solr(
        'http://127.0.0.1:8983/solr/gettingstarted/', always_commit=False)

    solr.ping()
    solr.delete(q='*:*')

    dist_programatically = list()
    sorted_list = list()

    x = list()
    y = sample_floats(-2.00, 2.00, k=128)

    doc_list = list()

    documents = int(input('Enter the number of documents to index : '))
    if documents is None:
        documents = 0

    print('Inserting records')

    for i in tqdm(range(1, documents + 1)):

        x = sample_floats(-2.00, 2.00, k=128)

        doc = {"id": i, "project_id": 1, "image_link": i, "v0_f": x[0], "v1_f": x[1], "v2_f": x[2], "v3_f": x[3], "v4_f": x[4],
               "v5_f": x[5], "v6_f": x[6], "v7_f": x[7], "v8_f": x[8], "v9_f": x[9], "v10_f": x[10], "v11_f": x[11],
               "v12_f": x[12], "v13_f": x[13], "v14_f": x[14], "v15_f": x[15], "v16_f": x[16], "v17_f": x[17], "v18_f": x[18],
               "v19_f": x[19], "v20_f": x[20], "v21_f": x[21], "v22_f": x[22], "v23_f": x[23], "v24_f": x[24], "v25_f": x[25],
               "v26_f": x[26], "v27_f": x[27], "v28_f": x[28], "v29_f": x[29], "v30_f": x[30], "v31_f": x[31], "v32_f": x[32],
               "v33_f": x[33], "v34_f": x[34], "v35_f": x[35], "v36_f": x[36], "v37_f": x[37], "v38_f": x[38], "v39_f": x[39],
               "v40_f": x[40], "v41_f": x[41], "v42_f": x[42], "v43_f": x[43], "v44_f": x[44], "v45_f": x[45], "v46_f": x[46],
               "v47_f": x[47], "v48_f": x[48], "v49_f": x[49], "v50_f": x[50], "v51_f": x[51], "v52_f": x[52], "v53_f": x[53],
               "v54_f": x[54], "v55_f": x[55], "v56_f": x[56], "v57_f": x[57], "v58_f": x[58], "v59_f": x[59], "v60_f": x[60],
               "v61_f": x[61], "v62_f": x[62], "v63_f": x[63], "v64_f": x[64], "v65_f": x[65], "v66_f": x[66], "v67_f": x[67],
               "v68_f": x[68], "v69_f": x[69], "v70_f": x[70], "v71_f": x[71], "v72_f": x[72], "v73_f": x[73], "v74_f": x[74],
               "v75_f": x[75], "v76_f": x[76], "v77_f": x[77], "v78_f": x[78], "v79_f": x[79], "v80_f": x[80], "v81_f": x[81],
               "v82_f": x[82], "v83_f": x[83], "v84_f": x[84], "v85_f": x[85], "v86_f": x[86], "v87_f": x[87], "v88_f": x[88],
               "v89_f": x[89], "v90_f": x[90], "v91_f": x[91], "v92_f": x[92], "v93_f": x[93], "v94_f": x[94], "v95_f": x[95],
               "v96_f": x[96], "v97_f": x[97], "v98_f": x[98], "v99_f": x[99], "v100_f": x[100], "v101_f": x[101],
               "v102_f": x[102], "v103_f": x[103], "v104_f": x[104], "v105_f": x[105], "v106_f": x[106], "v107_f": x[107],
               "v108_f": x[108], "v109_f": x[109], "v110_f": x[110], "v111_f": x[111], "v112_f": x[112], "v113_f": x[113],
               "v114_f": x[114], "v115_f": x[115], "v116_f": x[116], "v117_f": x[117], "v118_f": x[118], "v119_f": x[119],
               "v120_f": x[120], "v121_f": x[121], "v122_f": x[122], "v123_f": x[123], "v124_f": x[124], "v125_f": x[125],
               "v126_f": x[126], "v127_f": x[127]}
        doc_list.append(doc)

        if len(doc_list) % 1000 == 0:
            print('Committing documents !!')
            solr.add(doc_list, commit=True)
            doc_list.clear()

        dist = distance.euclidean(x, y)
        dist_programatically.append(round(dist, 2))

    solr.add(doc_list, commit=True)
    print('Done inserting')

    for dist in sorted(dist_programatically):
        if len(sorted_list) < 5:
            sorted_list.append(dist)
        else:
            break

    v = ''
    for j in range(0, 128):
        v = v + 'v{}_f'.format(str(j)) + ','

    url = 'http://127.0.0.1:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,' + v + \
        '{}'.format(','.join(str(element) for element in y)) + ')&fl=image_link,euclidean_distance:$euclidean_distance&\
sort=$euclidean_distance%20asc&rows=5'

    t1 = time.perf_counter()
    connection = urlopen(url)
    t2 = time.perf_counter()

    print('Took {}s to calculate by query'.format((t2 - t1)))
    results = json.load(connection)

    results = results.get('response').get('docs')
    print('Total results {} from query'.format(len(results)))

    print('Distance calculated programtically = ', sorted_list)

    # Just loop over it to access the results.
    for result in results:
        print(result.get('image_link'), result.get('euclidean_distance'))


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
    connect()


"""Auto commits: https://medium.com/tech-tajawal/tips-and-tricks-to-maximize-apache-solr-performance-74e8ea4f5c8d"""
