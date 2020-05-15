#!/bin/bash

resdb_path=$1
nodes=$2
clients=$3
max_inf=$4
bsize=100
SCRIPTPATH=$(pwd)

echo "Creating config file for compiling code.." >${SCRIPTPATH}/status.log
cd $resdb_path
source ./scripts/make_config.sh $nodes $clients $max_inf
if [ $? -eq 0 ]; then
    echo "Config file has been created.." >>${SCRIPTPATH}/status.log
else
    exit 0
fi

echo "Compiling the code..." | cat >>${SCRIPTPATH}/status.log
make clean
make -j8

if [ $? -eq 0 ]; then
    echo "Code has been compiled successfully." | cat >>${SCRIPTPATH}/status.log
else
    echo "Compile error." | cat >>${SCRIPTPATH}/status.log
    exit 0
fi

rm ifconfig.txt hostnames.py
echo "hostip = [" >>hostnames.py

for i in $(seq 1 $nodes); do
    ip=$(sed "${i}q;d" ${SCRIPTPATH}/8_core_ips.txt)
    echo $ip >>ifconfig.txt
    echo -e "\""${ip}"\"," >>hostnames.py
done

for i in $(seq 1 $clients); do
    ip=$(sed "${i}q;d" ${SCRIPTPATH}/4_core_ips.txt)
    echo $ip >>ifconfig.txt
    echo -e "\""${ip}"\"," >>hostnames.py
done
echo "]" >>hostnames.py
echo "hostmach = [" >>hostnames.py
for i in $(seq 1 $nodes); do
    ip=$(sed "${i}q;d" ${SCRIPTPATH}/8_core_ips.txt)
    echo -e "\""${ip}"\"," >>hostnames.py
done

for i in $(seq 1 $clients); do
    ip=$(sed "${i}q;d" ${SCRIPTPATH}/4_core_ips.txt)
    echo -e "\""${ip}"\"," >>hostnames.py
done
echo "]" >>hostnames.py

# Copy to scripts
cp run* scripts/
cp ifconfig.txt scripts/
cp config.h scripts/
cp hostnames.py scripts/
cd scripts

python3 simRun.py $nodes s${nodes}_c${clients}_results_PBFT_b${bsize}_run0_node 0 &
sleep 2
echo "ResilientDB has been started on ${nodes} nodes and ${clients}." >>${SCRIPTPATH}/status.log
echo "" >>${SCRIPTPATH}/status.log
for run in {1..30}; do
    i=$(($run * 2))
    sleep 2
    sed -i '$d' ${SCRIPTPATH}/status.log
    echo "warm up `echo "scale=2;($i / 60) * 100" | bc`%."  >>${SCRIPTPATH}/status.log
done

echo "" >>${SCRIPTPATH}/status.log

for run in {1..60}; do
    i=$(($run * 2))
    sleep 2
    sed -i '$d' ${SCRIPTPATH}/status.log
    echo "running `echo "scale=2;($i / 120) * 100" | bc`%." >>${SCRIPTPATH}/status.log
done
wait
rm ${SCRIPTPATH}/status.log
