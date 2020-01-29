#Script to monitor results. Database called grafana is created in InfluxDB and tables (measurements in InfluxDB) are created per node.
#Eg: R1 corresponds to 1st replica (server) and C1 to 1st client.
#A column for each criteria is created (currently only throughput) and populated when a result for the criteria is returned from binaries.
#The first column in each table is a timestamp as InfluxDB is a time series database.
#The grafana database is used to create graphs in grafana setup in the development machine.
#!/bin/bash
DEV_MACHINE_IP="$1"
touch ../toInflux_dummy_.out
curl -i -XPOST 'http://'$DEV_MACHINE_IP':8086/query' --data-urlencode "q=CREATE DATABASE grafana"
while true; do
    echo "In monitorResults.sh"
    filename=$(inotifywait --format '%w%f' -e modify ../toInflux_*_.out)
    echo $filename
    node=$(echo $filename | cut -f 2 -d '_')
    echo $node
    IFS=','
    throughput=$(tail -1 $filename)
    echo "tp - $throughput"
    curl -i -XPOST 'http://'$DEV_MACHINE_IP':8086/write?db=grafana' --data-binary $node' throughput='$throughput
    done
    if test -f "../toInflux_dummy_.out"; then
        rm ../toInflux_dummy_.out
    fi
done