#!/bin/bash
ip="35.238.169.30"
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" deps/ expo@$ip:resilientdb/deps;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" transport/ expo@$ip:resilientdb/transport;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" system/ expo@$ip:resilientdb/system;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" config.h expo@$ip:resilientdb/config.h;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" Makefile expo@$ip:resilientdb/Makefile;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" scripts/ expo@$ip:resilientdb/scripts;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" smart_contracts/ expo@$ip:resilientdb/smart_contracts;
#rsync -av statistics/ expo@$ip:resilientdb/statistics;
#rsync -av db/ expo@$ip:resilientdb/db;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" benchmarks/ expo@$ip:resilientdb/benchmarks;
rsync -av -e "ssh -i ~/.ssh/google_compute_engine" client/ expo@$ip:resilientdb/client;