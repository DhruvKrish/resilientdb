import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

arguments = len(sys.argv) - 1
if arguments != 2:
    print("Expects 2 Argument")
    print("create_line_chart.py <bool: compare with resilientdb> <csv_file_path>")
    exit(0)
juxt = int(sys.argv[1])
file_name = sys.argv[-1]
df = pd.read_csv(file_name, header=None)

# split dataset based on empty row
df_list = np.split(df, df[df.isnull().all(1)].index)

batch_size = []
cross_shard_per = []
tput = []
cores = df_list[0].iloc[1][1]
client = df_list[0].iloc[2][1]
replicas = 8
shard_size = 4
# check if replicas column present in csv
if df_list[0].iloc[3][0] == 'Replicas':
    replicas = int(df_list[0].iloc[3][1])
for i in range(0, len(df_list)):
    # reset index and delete empty row
    if i is not 0:
        df_list[i].reset_index(drop=True, inplace=True)
        df_list[i].drop(df_list[i].index[0], inplace=True)
    batch_size.append(int(df_list[i].iloc[0][1]))
    cross_shard_per.append(int(df_list[i].iloc[3][1]))
    total_tput = 0
    # sum tput for each shard
    for j in range(int(replicas/shard_size)):
        total_tput += df_list[i].iloc[7][j*shard_size + 1: j*shard_size + shard_size + 1].astype(float).mean()
    # shard_1_tput = df_list[i].iloc[6][1:5].astype(float).mean()
    # shard_2_tput = df_list[i].iloc[6][5:9].astype(float).mean()
    # total_tput = shard_1_tput + shard_2_tput
    tput.append(total_tput)

t50 = []
t100 = []
t1000 = []
# get unique batch sizes
b_size = set(batch_size)
cs_per = [0, 25, 50, 75, 100]
for i in range(0, len(cross_shard_per), len(b_size)):
    t50.append(tput[i])
    t100.append(tput[i + 1])
    t1000.append(tput[i + 2])

plt.figure(figsize=(8,8))
pt1 = plt.plot(cs_per, t50, label='Batch Size 50')
plt.scatter(cs_per, t50, marker="o")
pt2 = plt.plot(cs_per, t100, label='Batch Size 100')
plt.scatter(cs_per, t100, marker="v")
pt3 = plt.plot(cs_per, t1000, label='Batch Size 1000')
plt.scatter(cs_per, t1000, marker="^")
plt.legend()
title = "Cross Shard % vs. Throughput: " + str(replicas) + " Replicas " + str(client) + " Clients " + str(cores) + " cores"
plt.title(title)
plt.ylabel('Throughput')
plt.xlabel('Cross Shard %')
plt.xticks(cs_per)
# Juxtapose vanila ResilientDB, 4 replicas, 1 Client
if juxt == 1:
    if str(cores) == '4':
        # batch size 50
        plt.axhline(y=49336, linestyle='-.', color='#1f77b4')
        # batch size 100
        plt.axhline(y=52675, linestyle='-.', color='#ff7f0e')
        # batch size 1000
        plt.axhline(y=69095, linestyle='-.', color='#2ca02c')
    elif str(cores) == '8':
        # batch size 50
        plt.axhline(y=99718, linestyle='-.', color='#1f77b4')
        # batch size 100
        plt.axhline(y=118568, linestyle='-.', color='#ff7f0e')
        # batch size 1000
        plt.axhline(y=131642, linestyle='-.', color='#2ca02c')

out_file_name = ""
if juxt == 1:
    out_file_name = client + "_clients_" + cores + "_cores_juxt.png"
else:
    out_file_name = client + "_clients_" + cores + "_cores.png"
plt.savefig(out_file_name)
plt.show()
