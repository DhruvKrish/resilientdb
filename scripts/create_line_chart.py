import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv('4cores.csv', header=None)
df_list = np.split(df, df[df.isnull().all(1)].index)

batch_size = []
cross_shard_per = []
tput = []
for i in range(0, len(df_list)):
    if i is not 0:
        df_list[i].reset_index(drop=True, inplace=True)
        df_list[i].drop(df_list[i].index[0], inplace=True)
    batch_size.append(int(df_list[i].iloc[0][1]))
    cross_shard_per.append(int(df_list[i].iloc[3][1]))
    shard_1_tput = df_list[i].iloc[6][1:5].astype(float).mean()
    shard_2_tput = df_list[i].iloc[6][5:9].astype(float).mean()
    total_tput = shard_1_tput + shard_2_tput
    tput.append(total_tput)

t50 = []
t100 = []
t1000 = []
b_size = [50, 100, 1000]
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
plt.title('Cross Shard % vs. Throughput: 8 Replicas, 2 Clients, 4 cores')
plt.ylabel('Throughput')
plt.xlabel('Cross Shard %')
plt.xticks(cs_per)
plt.show()
