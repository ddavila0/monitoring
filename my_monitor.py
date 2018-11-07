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
    :condor_ad_type: the type of ClassAd to fetch e.g. htcondor.AdTypes.Schedd
    :projection: list of attributes to be queried
    :amq: the StompAMQ instance to be used to push the ads
    :pool: the pool name to be used in the metadata of the metric e.g "itb"
'''
def pull_and_push(collector, condor_ad_type, projection, amq, ad_type, pool, constraint="true"):
    ads = collector.query(condor_ad_type, constraint, projection)
    dict_list=convert_ads_to_dict_list(ads)
    timestamp=int(time.time())
    notifications=amq.make_notification(dict_list, "test_raw", pool, ad_type, timestamp)
    for notification in notifications:
        print(json.dumps(notification, sort_keys=True, indent=4))

    #failedNotifications=amq.send(notification)
    #for failed in failedNotifications:
    #    print("Failed")
    #    print(failed)




###############################################################################
#                                                                             #
#                               MAIN                                          #
#                                                                             #
###############################################################################

#-----------------------------------------------------------------------------#
# Setup basic configuration
#-----------------------------------------------------------------------------#

# Setup the logger and the log level {ERROR, INFO, DEBUG}
logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)

# Certificates used to push data through the AMQ service
my_cert="/etc/grid-security/hostcert.pem"
my_key="/etc/grid-security/hostkey.pem"

# Collector to be queried
CentralManagerMachine="vocms0809.cern.ch"
#-----------------------------------------------------------------------------#



#-----------------------------------------------------------------------------#
# Read projection list from files
#-----------------------------------------------------------------------------#

#projection_schedd=get_projection_from_file("classAds/schedd")
projection_schedd=get_projection_from_file("classAds/schedd.short")
log.debug("schedd projection:  %s", str(projection_schedd))

projection_startd=get_projection_from_file("classAds/startd")
projection_negotiator=get_projection_from_file("classAds/negotiator")
projection_collector=get_projection_from_file("classAds/collector")
projection_job=get_projection_from_file("classAds/job")
#-----------------------------------------------------------------------------#


#-----------------------------------------------------------------------------#
# Create collector and amq objects, to pull and push data
# respectively
#-----------------------------------------------------------------------------#

collector = htcondor.Collector(CentralManagerMachine)
collector9620 = htcondor.Collector(CentralManagerMachine+":9620")
amq=StompAMQ("","","monit_prod_cms_si_condor","/topic/cms.si.condor", [('dashb-test-mb.cern.ch', 61123)], logger=log, cert=my_cert, key=my_key, use_ssl=True)
#-----------------------------------------------------------------------------#


# Pull and Push data from Schedds
#pull_and_push(collector9620, htcondor.AdTypes.Schedd, projection_schedd, amq, "schedd", "itb")

# Pull and Push data from Negotiators
#pull_and_push(collector, htcondor.AdTypes.Negotiator, projection_negotiator, amq, "negotiator", "itb")

# Pull and Push data from Startds
#pull_and_push(collector, htcondor.AdTypes.Startd, projection_startd, amq, "startd", "itb")

# Pull and Push data from Collector (only from main collector, not backup nor ccb)
#const='Machine == "'+ CentralManagerMachine +'"'
#pull_and_push(collector, htcondor.AdTypes.Collector, projection_collector, amq, "collector", "itb", constraint=const)

# Pull and Push data from Autoclusters
coll=htcondor.Collector("vocms0809.cern.ch")
projection=['Machine','CondorPlatform', 'Name', 'AddressV1', 'MyAddress', 'CondorVersion']
schedd_ads =  coll.query(htcondor.AdTypes.Schedd, "true", projection)
for schedd_ad in schedd_ads:
    print(schedd_ad['name'])
    schedd = htcondor.Schedd(schedd_ad)
    try:
        #TODO:
            # - why it doesn't work with query()
            # - do we want all the autoclusters or only those idle?
        autoclusters=schedd.xquery("JobStatus==1", opts=htcondor.QueryOpts.AutoCluster)
    except RuntimeError :
        print "ERROR connecting to schedd", schedd_ad['Name']
        continue
    except:
        e = sys.exc_info()
        print("Unexpected exception: ")
        print(e)
        exit(1)
    else:
        print(len(list(autoclusters)))



#(htcondor.Schedd(schedd_ad).xquery(
#      requirements="JobStatus=?=1",opts=htcondor.QueryOpts.AutoCluster))

###############################################################################

#projection_collector=["Name", "Machine"]
#
#ads = collector.query(htcondor.AdTypes.Collector, "Machine==\"vocms0809.cern.ch\"", projection_collector)
##ads = collector.query(htcondor.AdTypes.Collector, "true", projection_collector)
#print(ads)


 
#failedNotifications=amq.send(notification)
#for failed in failedNotifications:
#    print("Failed")
#    print(failed)





#repls = ('hello', 'goodbye'), ('world', 'earth')
#s = 'hello, world'
## s - is the initial value
#ss=reduce(lambda a, kv: a.replace(*kv), repls, s)





	


#
#pilot_projection=["State","Activity","SlotType","CPUs","Memory","Disk",
#  "MyCurrentTime","GLIDEIN_CMSSite","GLIDEIN_ToDie","GLIDEIN_ToRetire",
#  "GLIDEIN_Job_Max_Time","GLIDEIN_MAX_Walltime","GLIDECLIENT_Name",
#  "DaemonStartTime","GLIDEIN_Schedd","GlobalJobId","Repackslots",
#  "DetectedRepackslots","Ioslots","DetectedIoslots","GLIDECLIENT_Group",
#  "MyType"]
#
#nego_projection=["Name","LastNegotiationCycleDuration0","MyCurrentTime",
#  "MyType"]
#
#schedd_projection=["Name","MaxJobsRunning","TotalRunningJobs",
#  "TotalIdleJobs","TotalHeldJobs","ServerTime","MyType",
#  "RecentDaemonCoreDutyCycle"]
#
#Collector=htcondor.Collector(CentralManagerMachine)
#Collector9620=htcondor.Collector(CentralManagerMachine+":9620")
#
## Negotiator query
#nego_ads=Collector.query(htcondor.AdTypes.Negotiator,"true",nego_projection)
#
## Schedd query
#schedd_ads=Collector9620.query(htcondor.AdTypes.Schedd,"true",schedd_projection)
#
## Pilot query
#startd_ads=Collector.query(htcondor.AdTypes.Startd,"true",pilot_projection)
#
## Idle AutoCluster non-blocking xquery's - projections do not work.
#queries = []
#for schedd_ad in Collector.locateAll(htcondor.DaemonTypes.Schedd) :
#  try :
#    queries.append(htcondor.Schedd(schedd_ad).xquery(
#      requirements="JobStatus=?=1",opts=htcondor.QueryOpts.AutoCluster))
#  except RuntimeError :
#    print "ERROR connecting to schedd", schedd_ad['Name']
#autocluster_ads=[]
#for query in htcondor.poll(queries):
#  ads=query.nextAdsNonBlocking()
#  for ad in ads :
#    autocluster_ads.append(ad)
#
## Output the results to json file
#
#timestamp=int(time.time())
#OUTPUTFILE=str(CentralManagerMachine)+"."+str(timestamp)+".js"
#file=open(OUTPUTFILE,'w')
#
#print >> file, "["
#for record in startd_ads+nego_ads+schedd_ads+autocluster_ads :
#  convert_ClassAd_to_json(record,file)
#print >> file, "{\n  \"MyType\" : \"Junk\"\n}\n]"
#file.close()
#
## Verify that the output file is parsable json
#try :
#  json_data=open(OUTPUTFILE,"r")
#except IOError :
#  print >> sys.stderr, "Unable to open file: "+OUTPUTFILE
#  sys.exit(4)
#try :
#  data = json.load(json_data)
#  print >> sys.stderr, "json output verified: "+OUTPUTFILE
#except ValueError :
#  print >> sys.stderr, "Unable to decode json file: "+OUTPUTFILE
#  sys.exit(5)
#json_data.close()
#
#print data
#
#sys.exit()
