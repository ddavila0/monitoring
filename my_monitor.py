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
'''
def pull_and_push(collector, condor_ad_type, projection, amq, ad_type, pool, constraint="true"):
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
    #for notification in notifications:
    #    print(json.dumps(notification, sort_keys=True, indent=4))

    amq.send(notifications)

def pull_and_push_autoclusters(collector, projection, amq, ad_type, pool, constraint="true"):
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
            amq.send(notifications)
            #for notification in notifications:
            #    print(json.dumps(notification, sort_keys=True, indent=4))



###############################################################################
#                                                                             #
#                               MAIN                                          #
#                                                                             #
###############################################################################

#-----------------------------------------------------------------------------#
# Setup basic configuration
#-----------------------------------------------------------------------------#

# Setup the logger and the log level {ERROR, INFO, DEBUG, WARNING}
logging.basicConfig(level=logging.ERROR)
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

#-----------------------------------------------------------------------------#

#-----------------------------------------------------------------------------#
# Read collector name from arguments
#-----------------------------------------------------------------------------#

if len(sys.argv) != 2:
    log.error("Incorrect number of arguments, expecting collector name and got instead:  %s", sys.argv)
    exit(1)
collector_name=sys.argv[1]


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
pull_and_push(collector9620, htcondor.AdTypes.Schedd, projection_schedd, amq, "schedd", "itb")

# Pull and Push data from Negotiators
pull_and_push(collector, htcondor.AdTypes.Negotiator, projection_negotiator, amq, "negotiator", "itb")

# Pull and Push data from Startds
pull_and_push(collector, htcondor.AdTypes.Startd, projection_startd, amq, "startd", "itb")

# Pull and Push data from Collector (only from main collector, not backup nor ccb)
const='Machine == "'+ collector_name +'"'
pull_and_push(collector, htcondor.AdTypes.Collector, projection_collector, amq, "collector", "itb", constraint=const)

# Pull and Push data from Autoclusters
const='JobStatus == 1'
pull_and_push_autoclusters(collector9620, projection_autocluster, amq, "autocluster", "itb", constraint=const)

###############################################################################

