#!/usr/bin/python
import htcondor
import classad
import json
import time
from StompAMQ import StompAMQ
from my_utils import convert_ClassAd_to_json
import logging 
import pdb
import sys

''' 
	Reads the  attributes in <filename> and creates a list
	:param filename: path to file with a list of classAd atttrbiutes
	:return: a list of classAd attributes to be used as projection
'''
def get_projection_from_file(filename):
	attr_list=[]
	fd = open(filename, 'r')
	for attr in fd.readlines():
		attr_list.append(attr[:-1])

	return attr_list

def convert_ads_to_dict_list(ads):
    # TODO: make convertion to dictionary directly
    dict_list=[]
    for ad in ads:
        #TODO:
            # - fix convert_ClassAd_to_json to not append last comma
        json_ad=convert_ClassAd_to_json(ad)
        dict_ad=json.loads(json_ad[:-1])
        dict_list.append(dict_ad)

    return dict_list

'''
    Queries the <collector> for ads of the type <condor_ad_type> using
    the projection in <projection> and push them to the AMQ service
    instance represented by <amq>
    :param collector: htcondor collector object to be queried
    :param condor_ad_type: the type of ClassAd to fetch e.g. htcondor.AdTypes.Schedd
    :param projection: list of attributes to be queried
    :param amq: the StompAMQ instance to be used to push the ads
    :param ad_type: the "type" parameter in the metric e.g "schedd"
    :param pool: the pool name to be used in the metadata of the metric e.g "itb"
    :param constraint: the constraint used to query the condor daemon
    :param output_action: push: push the ads, print: print the ads, both: push and print the ads
'''
def pull_and_push(collector, condor_ad_type, projection, amq, ad_type, pool, output_action, constraint="true"):
    ads = collector.query(condor_ad_type, constraint, projection)
    dict_list=convert_ads_to_dict_list(ads)
    # Time in miliseconds is required
    timestamp=int(time.time()*1000)
    metadata={'timestamp' : timestamp,
              'producer' : "cms",
              'type' : "si_condor_"+ad_type,
              'type_prefix' : "raw",
              'version' : "0.2",
              'pool' : pool}

    notifications=amq.make_notification(dict_list, metadata)
    # Print the collected data
    if output_action == "both" or output_action == "print":
        for notification in notifications:
            print(json.dumps(notification, sort_keys=True, indent=4))
    # Push the collected data
    if output_action == "both" or output_action == "push":
        amq.send(notifications)

def pull_and_push_autoclusters(collector, projection, amq, ad_type, pool, output_action, constraint="true"):
    projection_aux=['Machine','CondorPlatform', 'Name', 'AddressV1', 'MyAddress', 'CondorVersion']
    schedd_ads =  collector.query(htcondor.AdTypes.Schedd, "true", projection_aux)
    # Time in miliseconds is required
    timestamp=int(time.time()*1000)
    for schedd_ad in schedd_ads:
        schedd_name=schedd_ad['name']
        schedd = htcondor.Schedd(schedd_ad)
        try:
            #TODO:
                # - why it doesn't work with query()
                # - do we want all the autoclusters or only those idle?
            ads=schedd.xquery(constraint, opts=htcondor.QueryOpts.AutoCluster)
        except RuntimeError :
            log.error("Connecting to schedd: %s", schedd_ad['Name'])
            continue
        except:
            e = sys.exc_info()
            log.error("Unexpected exception: %s", str(e))
            exit(1)
        else:
            dict_list=convert_ads_to_dict_list(ads)
            metadata={'timestamp' : timestamp,
              'producer' : "cms",
              'type' : "si_condor_"+ad_type,
              'type_prefix' : "raw",
              'version' : "0.2",
              'pool' : pool,
              'schedd' : schedd_name}

            notifications=amq.make_notification(dict_list, metadata)
            # Print the collected data
            if output_action == "both" or output_action == "print":
                for notification in notifications:
                    print(json.dumps(notification, sort_keys=True, indent=4))
            # Push the collected data
            if output_action == "both" or output_action == "push":
                amq.send(notifications)


'''
    Pick from a list of HA collectors, the one running the negotiator(s) daemons
    :param list_of_collectors : the list of HA collectors
    :return: one item from the <list_of_collectors>
'''
def get_main_collector(list_of_collectors):

    # If the list is empty, exit
    if len(list_of_collectors) == 0:
        log.error("List of collectors is empty")
        exit(6)

    main_collector=""
    for collector_name in list_of_collectors:
        collector = htcondor.Collector(collector_name)
        ads=collector.query(htcondor.AdTypes.Negotiator, "true", ["Name"])
        if len(ads) !=0:
            # more than one collector running negotiator(s)
            if main_collector != "":
                log.warning("Negotiators are running in both HA hosts")
            main_collector=collector_name
            log.warning("Cannot find any negotiator running")
        # if no negotiator running anywhere, pick the first collector
        if main_collector == "":
            main_collector=list_of_collectors[0]

    return main_collector


###############################################################################
#                                                                             #
#                               MAIN                                          #
#                                                                             #
###############################################################################

#-----------------------------------------------------------------------------#
# Setup basic configuration
#-----------------------------------------------------------------------------#

# Setup the logger and the log level {ERROR, INFO, DEBUG, WARNING}
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Monit specific configuration
monit_end_point=[('dashb-mb.cern.ch', 61123)]
monit_producer="cms"
monit_topic="/topic/cms.si.condor"
# username and passwrod aren't necessary when using certificates
monit_username=""
monit_password=""

# Certificates used to push data through the AMQ service
my_cert="/etc/grid-security/hostcert.pem"
my_key="/etc/grid-security/hostkey.pem"

# Pool name - Collector mapping
# TODO:
#   - Move to a config file
pool_collector_map={"itbdev":   ["vocms0804.cern.ch"],
                    "itb" :     ["cmsgwms-collector-itb.cern.ch", "cmssrv215.fnal.gov"],
                    "global" :  ["cmsgwms-collector-global.cern.ch","cmssrv221.fnal.gov"],
                    "cern" :    ["cmsgwms-collector-tier0.cern.ch", "cmssrv239.fnal.gov"]}

#-----------------------------------------------------------------------------#

#-----------------------------------------------------------------------------#
# Read collector name from arguments
#-----------------------------------------------------------------------------#
# First argument must be the pool name (itbdev, itb, global, or cern)
# Second argument denotes the output action of the script:
#   - push: pushes the collected data to ES
#   - print: only print the collected data but don't push it
#   - both: push and print

if len(sys.argv) < 2 or len(sys.argv) > 3:
    log.error("Incorrect number of arguments, expecting: pool_name [output_action] and got instead: %s", sys.argv)
    exit(1)

output_action="push"
action_list=["push","print","both"]
if len(sys.argv) == 3:
    if sys.argv[2] in action_list:
        output_action=sys.argv[2]
    else:
        log.error("Incorrect ouput_action, expecting: push, print or both and got instead: %s", sys.argv[2])
        exit(2)

# Is pool in pool map?
if sys.argv[1] in pool_collector_map:
    pool_name=sys.argv[1]
else:
    log.error("Incorrect pool name, expecting: \
               %s  and got instead:  %s", str(pool_collector_map.keys()), sys.argv[2])
    exit(3)


# Get a list of collector names from the pool, using the <pool_collector_map>
list_of_collectors=pool_collector_map[pool_name]

# Pick the main collector (the one running the
collector_name=get_main_collector(list_of_collectors) 

log.info("\n Pool name: %s\n Output action: %s\n Collector name: %s\n", pool_name, output_action, collector_name)
exit(0)

#-----------------------------------------------------------------------------#
# Read projection list from files
#-----------------------------------------------------------------------------#

projection_schedd=get_projection_from_file("classAds/schedd")
projection_startd=get_projection_from_file("classAds/startd")
projection_negotiator=get_projection_from_file("classAds/negotiator")
projection_collector=get_projection_from_file("classAds/collector")
projection_autocluster=get_projection_from_file("classAds/autocluster")
#-----------------------------------------------------------------------------#


#-----------------------------------------------------------------------------#
# Create collector and amq objects, to pull and push data
# respectively
#-----------------------------------------------------------------------------#

collector = htcondor.Collector(collector_name)
collector9620 = htcondor.Collector(collector_name+":9620")


amq=StompAMQ(monit_username, monit_password, monit_producer, monit_topic, monit_end_point, logger=log, cert=my_cert, key=my_key, use_ssl=True)
#-----------------------------------------------------------------------------#


# Pull and Push data from Schedds
log.info("Pushing data from schedds")
pull_and_push(collector9620, htcondor.AdTypes.Schedd, projection_schedd, amq, "schedd", pool_name, output_action)

# Pull and Push data from Negotiators
log.info("Pushing data from negotiator(s)")
pull_and_push(collector, htcondor.AdTypes.Negotiator, projection_negotiator, amq, "negotiator", pool_name, output_action)


# Pull and Push data from Startds
log.info("Pushing data from startds")
pull_and_push(collector, htcondor.AdTypes.Startd, projection_startd, amq, "startd", pool_name, output_action)


# Pull and Push data from Collector (only from main collector, not backup nor ccb)
log.info("Pushing data from collector")
const='Machine == "'+ collector_name +'"'
pull_and_push(collector, htcondor.AdTypes.Collector, projection_collector, amq, "collector", pool_name, output_action, constraint=const)

# Pull and Push data from Autoclusters
log.info("Pushing data from autoclusters")
const='JobStatus == 1'
pull_and_push_autoclusters(collector9620, projection_autocluster, amq, "autocluster", pool_name, output_action, constraint=const)

###############################################################################

