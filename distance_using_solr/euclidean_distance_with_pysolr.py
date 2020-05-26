import pysolr
from urllib.request import urlopen
import random
from scipy.spatial import distance
import json
import time


def connect():
    # Setup a Solr instance. The timeout is optional.
    solr = pysolr.Solr(
        'http://127.0.0.1:8983/solr/gettingstarted/', always_commit=False)

    solr.ping()
    solr.delete(q='*:*')

    dist_programatically = list()
    sorted_list = list()

    x = list()
    y = [-0.44, -1.25, 1.56, 0.98, -0.37, 1.19, -1.24, -0.22, -0.79, -0.12, 1.2, -1.93, -1.63, -1.87, -1.01, -1.29, -1.17, -1.33,
        -1.12, 0.79, 0.2, -0.45, 1.31, 1.21, 0.38, -0.76, -0.71, 0.42, -0.39, 0.39, 1.11, -1.46, 1.43, 1.76, 1.28, 1.62, 1.07, 
        -1.66, 0.31, -1.53, 1.65, -0.88, 0.77, 1.31, -0.43, -0.69, -1.16, -0.56, -0.83, -0.17, -0.05, -0.47, 1.33, 1.37, 1.39, 
        -1.45, 0.11, 1.43, 1.25, 0.52, -1.89, -0.41, -1.89,1.93, -1.32, 1.18, 0.06, 1.38, -0.46, 0.41, -0.37, -0.86, 1.52, -0.84, 
        -1.17, -1.8, 1.0, -0.15, -0.58, -1.18, 0.25, -0.15, -0.05, 0.94, -0.45, -0.19, 0.07, -1.63, 1.2, 1.25, 0.76, 1.51, 0.31, 
        -1.34, -0.85, -0.82, -0.95, -0.12, -1.21, 1.0, -0.88, 1.58, -1.77, -1.45, 1.49, 0.36, -0.56, -0.54, 1.51, -0.29, 0.16, 
        -0.48, -1.88, 1.76, -1.5, -1.86, -0.92, 0.6, -1.87, -0.65, 1.6, -1.8, -1.02, 0.87, 0.56, 0.29, 0.2, 1.47]
    doc_list = list()

    print('Inserting records')

    for i in tqdm(range(1, 1000001)):
        x = sample_floats(-2.00, 2.00, k=128)
        doc = {"id": i, "project_id": 1, "image_link": i, "v0_f": x[0], "v1_f": x[1], "v2_f": x[2], "v3_f": x[3], "v4_f": x[4], "v5_f": x[5],
               "v6_f": x[6], "v7_f": x[7], "v8_f": x[8], "v9_f": x[9], "v10_f": x[10], "v11_f": x[11], "v12_f": x[12], "v13_f": x[13], "v14_f": x[14],
               "v15_f": x[15], "v16_f": x[16], "v17_f": x[17], "v18_f": x[18], "v19_f": x[19], "v20_f": x[20], "v21_f": x[21], "v22_f": x[22],
               "v23_f": x[23], "v24_f": x[24], "v25_f": x[25], "v26_f": x[26], "v27_f": x[27], "v28_f": x[28], "v29_f": x[29], "v30_f": x[30],
               "v31_f": x[31], "v32_f": x[32], "v33_f": x[33], "v34_f": x[34], "v35_f": x[35], "v36_f": x[36], "v37_f": x[37], "v38_f": x[38],
               "v39_f": x[39], "v40_f": x[40], "v41_f": x[41], "v42_f": x[42], "v43_f": x[43], "v44_f": x[44], "v45_f": x[45], "v46_f": x[46],
               "v47_f": x[47], "v48_f": x[48], "v49_f": x[49], "v50_f": x[50], "v51_f": x[51], "v52_f": x[52], "v53_f": x[53], "v54_f": x[54],
               "v55_f": x[55], "v56_f": x[56], "v57_f": x[57], "v58_f": x[58], "v59_f": x[59], "v60_f": x[60], "v61_f": x[61], "v62_f": x[62],
               "v63_f": x[63], "v64_f": x[64], "v65_f": x[65], "v66_f": x[66], "v67_f": x[67], "v68_f": x[68], "v69_f": x[69], "v70_f": x[70],
               "v71_f": x[71], "v72_f": x[72], "v73_f": x[73], "v74_f": x[74], "v75_f": x[75], "v76_f": x[76], "v77_f": x[77], "v78_f": x[78],
               "v79_f": x[79], "v80_f": x[80], "v81_f": x[81], "v82_f": x[82], "v83_f": x[83], "v84_f": x[84], "v85_f": x[85], "v86_f": x[86],
               "v87_f": x[87], "v88_f": x[88], "v89_f": x[89], "v90_f": x[90], "v91_f": x[91], "v92_f": x[92], "v93_f": x[93], "v94_f": x[94],
               "v95_f": x[95], "v96_f": x[96], "v97_f": x[97], "v98_f": x[98], "v99_f": x[99], "v100_f": x[100], "v101_f": x[101], "v102_f": x[102],
               "v103_f": x[103], "v104_f": x[104], "v105_f": x[105], "v106_f": x[106], "v107_f": x[107], "v108_f": x[108], "v109_f": x[109],
               "v110_f": x[110], "v111_f": x[111], "v112_f": x[112], "v113_f": x[113], "v114_f": x[114], "v115_f": x[115], "v116_f": x[116],
               "v117_f": x[117], "v118_f": x[118], "v119_f": x[119], "v120_f": x[120], "v121_f": x[121], "v122_f": x[122], "v123_f": x[123],
               "v124_f": x[124], "v125_f": x[125], "v126_f": x[126], "v127_f": x[127]}
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

    t1 = time.perf_counter()
    url = 'http://localhost:8983/solr/gettingstarted/select?q=*:*&euclidean_distance=dist(2,v0_f,v1_f,v2_f,v3_f,v4_f,v5_f,v6_f,\
v7_f,v8_f,v9_f,v10_f,v11_f,v12_f,v13_f,v14_f,v15_f,v16_f,v17_f,v18_f,v19_f,v20_f,v21_f,v22_f,v23_f,v24_f,v25_f,v26_f,v27_f,\
v28_f,v29_f,v30_f,v31_f,v32_f,v33_f,v34_f,v35_f,v36_f,v37_f,v38_f,v39_f,v40_f,v41_f,v42_f,v43_f,v44_f,v45_f,v46_f,v47_f,\
v48_f,v49_f,v50_f,v51_f,v52_f,v53_f,v54_f,v55_f,v56_f,v57_f,v58_f,v59_f,v60_f,v61_f,v62_f,v63_f,v64_f,v65_f,v66_f,v67_f,\
v68_f,v69_f,v70_f,v71_f,v72_f,v73_f,v74_f,v75_f,v76_f,v77_f,v78_f,v79_f,v80_f,v81_f,v82_f,v83_f,v84_f,v85_f,v86_f,v87_f,\
v88_f,v89_f,v90_f,v91_f,v92_f,v93_f,v94_f,v95_f,v96_f,v97_f,v98_f,v99_f,v100_f,v101_f,v102_f,v103_f,v104_f,v105_f,v106_f,\
v107_f,v108_f,v109_f,v110_f,v111_f,v112_f,v113_f,v114_f,v115_f,v116_f,v117_f,v118_f,v119_f,v120_f,v121_f,v122_f,v123_f,\
v124_f,v125_f,v126_f,v127_f,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\
{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\
{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\
{},{},{},{},{},{},{},{},{},{},{},{},{},{})&fl=image_link,euclidean_distance:$euclidean_distance&sort=$euclidean_distance%20\
asc&rows=5'.format(str(y[0]), str(y[1]), str(y[2]), str(y[3]), str(y[4]), str(y[5]), str(y[6]), str(y[7]), str(y[8]), str(y[9]), 
str(y[10]), str(y[11]), str(y[12]), str(y[13]), str(y[14]), str(y[15]), str(y[16]), str(y[17]), str(y[18]), str(y[19]), 
str(y[20]), str(y[21]), str(y[22]), str(y[23]), str(y[24]), str(y[25]), str(y[26]), str(y[27]), str(y[28]), str(y[29]), 
str(y[30]), str(y[31]), str(y[32]), str(y[33]), str(y[34]), str(y[35]), str(y[36]), str(y[37]), str(y[38]), str(y[39]), 
str(y[40]), str(y[41]), str(y[42]), str(y[43]), str(y[44]), str(y[45]), str(y[46]), str(y[47]), str(y[48]), str(y[49]),
 str(y[50]), str(y[51]), str(y[52]), str(y[53]), str(y[54]), str(y[55]), str(y[56]), str(y[57]), str(y[58]), str(y[59]), 
 str(y[60]), str(y[61]), str(y[62]), str(y[63]), str(y[64]), str(y[65]), str(y[66]), str(y[67]), str(y[68]), str(y[69]), 
 str(y[70]), str(y[71]), str(y[72]), str(y[73]), str(y[74]), str(y[75]), str(y[76]), str(y[77]), str(y[78]), str(y[79]), 
 str(y[80]), str(y[81]), str(y[82]), str(y[83]), str(y[84]), str(y[85]), str(y[86]), str(y[87]), str(y[88]), str(y[89]), 
 str(y[90]), str(y[91]), str(y[92]), str(y[93]), str(y[94]), str(y[95]), str(y[96]), str(y[97]), str(y[98]), str(y[99]), 
 str(y[100]), str(y[101]), str(y[102]), str(y[103]), str(y[104]), str(y[105]), str(y[106]), str(y[107]), str(y[108]), 
 str(y[109]), str(y[110]), str(y[111]), str(y[112]), str(y[113]), str(y[114]), str(y[115]), str(y[116]), str(y[117]), 
 str(y[118]), str(y[119]), str(y[120]), str(y[121]), str(y[122]), str(y[123]), str(y[124]), str(y[125]), str(y[126]), 
 str(y[127]))

    print('URL :\n', url)

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
