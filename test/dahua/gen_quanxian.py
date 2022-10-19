import csv
import numpy as np

# file_name = "quanxian.csv"
# with open(file_name, "w") as csv_file:
#     writer = csv.writer(csv_file, delimiter=',')
#     for i in range(200000):
#         a = np.random.randint(0, 2, 400)
#         for j in range(400):
#             line = [i, str(j), a[j]]
#             writer.writerow(line)

with open("id_card_id.txt", "r") as f:
    a = f.readlines()

for i in range(len(a)):
    if i == 0:
        continue
    face = a[i].split("	")[0]