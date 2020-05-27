import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

cores = ""
t_putXper = []
for i in range(2, 7):
    file_name = str(i) + 'shards_8.csv'
    df = pd.read_csv(file_name, header=None)
    tput = []
    df_list = np.split(df, df[df.isnull().all(1)].index)
    cores = df_list[0].iloc[1][1]
    replicas = int(df_list[0].iloc[3][1])
    shard_size = 4
    for j in range(0, len(df_list)):
        if j is not 0:
            df_list[j].reset_index(drop=True, inplace=True)
            df_list[j].drop(df_list[j].index[0], inplace=True)
        total_tput = 0
        # sum tput for each shard
        for k in range(int(replicas/shard_size)):
            total_tput += df_list[j].iloc[7][k*shard_size + 1: k*shard_size + shard_size + 1].astype(float).mean()
        tput.append(total_tput)
    t_putXper.append(tput)

no_shards = [2, 3, 4, 5, 6]
cross_shard_per = [0, 25, 50, 75, 100]
plt.figure(figsize=(8,8))
for i in no_shards:
    label = str(cross_shard_per[i - 2]) + " % Cross Shard"
    temp = [t[i - 2] for t in t_putXper]
    plt.plot(no_shards, temp, label=label)
    plt.scatter(no_shards, temp)
plt.legend()
title = "No of Shards % vs. Throughput (" + str(cores) + " cores)"
plt.title(title)
plt.ylabel('Throughput')
plt.xlabel('No of Shards')
plt.xticks(no_shards)
out_file_name = "shards_vs_throughput_" + str(cores) + "_cores"
plt.savefig(out_file_name)
plt.show()