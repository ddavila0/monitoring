#!/bin/bash
cd /data/srv/monitoring/monit

# Monitor the itb pool
python my_monitor.py itb push 1>> log/itb/log.out 2>> log/itb/log.err
python my_monitor.py global push 1>> log/global/log.out 2>> log/global/log.err

