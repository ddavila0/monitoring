#!/bin/bash
cd /data/srv/ddavila/monitoring

# Monitor the itb pool
python my_monitor.py vocms0809.cern.ch 1>> log/log.out 2>> log/log.err

