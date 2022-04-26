result = {}
csv = {}
with open("../euler_db2.log", 'r')  as f:
    a = f.readlines()
    for i in range(0, len(a), 6):
        if i == 0:
            continue
        if a[i][:3] == "SQL":
            k = a[i].replace("\n", "")
            if k[4:] not in result:
                result[k[4:]] = [[], [], [], []]
                csv[k[4:]] = []
            all_time = 0
            for j in range(4):
                v = a[i+j+2].replace("\n", "")
                vv = v.split(":")[1]
                result[k[4:]][j].append(vv)
            r = (a[i+2].replace("\n", "").split(":")[1], a[i+3].replace("\n", "").split(":")[1],
                 a[i+4].replace("\n", "").split(":")[1], a[i+5].replace("\n", "").split(":")[1])
            csv[k[4:]].append(r)
    print(result.keys())

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import style

# matplotlib.rcParams['text.usetex'] = True
plt.figure(figsize=(10, 10), dpi=70)

x = []
for i in range(100000, 10100000, 100000):
    x.append(i)
with open("result.csv", "w+") as f:
    f.write("数据量, 加密时间, 加密SQL执行时间, 解密时间, MYSQL时间" + "\n")
    for k, v in csv.items():
        print(k)
        num = 10
        for i in v:
            f.write(str(num) + "w, " + i[0] + ", " + i[1] + ", " + i[2] + ", " + i[3] + "\n")
            num += 10
        f.write(", , , ," + "\n")

for k, v in result.items():
    y = [float(i) for i in v[1]]
    y1 = [float(i) for i in v[3]]
    plt.plot(x, y, '-p', color='grey',
        marker = '.',
        markersize=4, linewidth=1,
        markerfacecolor='red',
        markeredgecolor='grey',
        markeredgewidth=2)
    plt.plot(x, y1, '-p', color='red',
             marker='.',
             markersize=4, linewidth=1,
             markerfacecolor='red',
             markeredgecolor='grey',
             markeredgewidth=2)
    plt.show()
# plt.savefig("test.png")








