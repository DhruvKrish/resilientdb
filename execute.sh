touch toInflux_dummy_.out
curl -i -XPOST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE grafana"
while true; do
    echo "In execute.sh"
    filename=$(inotifywait --format '%w%f' -e modify toInflux_*_.out)
    echo $filename
    sh InfluxDB.sh $filename
    if test -f "toInflux_dummy_.out"; then
        rm toInflux_dummy_.out
    fi
done
