#!/bin/bash
cd /data/srv/ddavila/monitoring

# Monitor the itb pool
python my_monitor.py itb push 1>> log/itb/log.out 2>> log/itb/log.err

