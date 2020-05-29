import sys
import csv
from datetime import date

arguments = len(sys.argv) - 1
if arguments != 6:
    print("Expects 6 arguments")
    print("create_log_csv.py <no_of_replicas> <no_of_clients> <batch_size> <no_of_cores> <cross_shard %> <csv_name>")
    exit(0)
# set variable values
csv_file_name = sys.argv[-1] + '.csv'
replicas = int(sys.argv[1])
clients = int(sys.argv[2])
batch_size = int(sys.argv[3])
cores = int(sys.argv[4])
cs_per = int(sys.argv[5])
path = 's' + str(replicas) + '_c' + str(clients) + '_results_PBFT_b' + str(batch_size) + '_run0_node'
with open(csv_file_name, 'a+', newline='') as csvFile:
    writer = csv.writer(csvFile)
    writer.writerow(['Batch Size', batch_size])
    writer.writerow(['Core',cores])
    writer.writerow(['Clients', clients])
    writer.writerow(['Replicas', replicas])
    writer.writerow(['Cross Shard %', cs_per])
    today = date.today()
    writer.writerow(['Run Date & Time', today])
    nodes = ['Node']
    tput = ['tput']
    worker0_idle = ['Worker Thread 0']
    worker1_idle = ['Worker Thread 1']
    worker2_idle = ['Worker Thread 2']
    worker3_idle = ['Worker Thread 3']
    worker4_idle = ['Worker Thread 4']
    virtual_mem = ['Virtual Memory']
    physical_mem = ['Physical Memory']
    for i in range(0, replicas):
        nodes.append(i)
        curr_path = path + str(i) + '.out'
        with open(curr_path, 'r', encoding="ISO-8859-1") as f:
            for line in (f.readlines() [-45:-3]):
                line_str = str(line)
                if "tput" in line_str:
                    tput.append(line_str.split('=')[-1].strip('\n'))
                if "idle_time_worker 0" in line_str:
                    worker0_idle.append(line_str.split('=')[-1].strip('\n'))
                if "idle_time_worker 1" in line_str:
                    worker1_idle.append(line_str.split('=')[-1].strip('\n'))
                if "idle_time_worker 2" in line_str:
                    worker2_idle.append(line_str.split('=')[-1].strip('\n'))
                if "idle_time_worker 3" in line_str:
                    worker3_idle.append(line_str.split('=')[-1].strip('\n'))
                if "idle_time_worker 4" in line_str:
                    worker4_idle.append(line_str.split('=')[-1].strip('\n'))
                if "virt_mem_usage" in line_str:
                    virtual_mem.append(line_str.split('=')[-1].strip('\n'))
                if "phys_mem_usage" in line_str:
                    physical_mem.append(line_str.split('=')[-1].strip('\n'))
    writer.writerow(nodes)
    writer.writerow(tput)
    writer.writerow(worker0_idle)
    writer.writerow(worker1_idle)
    writer.writerow(worker2_idle)
    writer.writerow(worker3_idle)
    writer.writerow(worker4_idle)
    writer.writerow(virtual_mem)
    writer.writerow(physical_mem)
    writer.writerow([''])
