#!/bin/bash
cd /data/srv/monitoring/monit

# Monitor the itb pool
python my_monitor.py itb push 1>> log/itb/log.out 2>> log/itb/log.err
# Monitor the global pool
python my_monitor.py global push 1>> log/global/log.out 2>> log/global/log.err
# Monitor the cern pool
python my_monitor.py cern push 1>> log/cern/log.out 2>> log/cern/log.err

