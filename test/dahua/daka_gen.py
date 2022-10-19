import csv
import datetime

file_name = "daka.csv"
with open(file_name, "w") as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    for i in range(1000000):
        line = [i%200000, i%400, str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
        writer.writerow(line)