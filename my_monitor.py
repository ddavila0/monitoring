#!/usr/bin/python
import htcondor
import classad
import json
import time
from StompAMQ import StompAMQ
from my_utils import convert_ClassAd_to_json
import logging 
import pdb

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
my_cert="/etc/grid-security/hostcert.pem"
my_key="/etc/grid-security/hostkey.pem"

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
   

CentralManagerMachine="vocms0809.cern.ch"
#projection_schedd=get_projection_from_file("classAds/schedd")
projection_schedd=get_projection_from_file("classAds/schedd.short")
projection_startd=get_projection_from_file("classAds/startd")
projection_negotiator=get_projection_from_file("classAds/negotiator")
projection_collector=get_projection_from_file("classAds/collector")
projection_job=get_projection_from_file("classAds/job")

#log.debug("schedd projection:  %s", str(projection_schedd))
#log.debug("startd projection:  %s", str(projection_startd))
#log.debug("negotiator projection:  %s", str(projection_negotiator))
#log.debug("collector projection:  %s", str(projection_collector))
#log.debug("job projection:  %s", str(projection_job))

collector = htcondor.Collector(CentralManagerMachine)
collector9620 = htcondor.Collector(CentralManagerMachine+":9620")

ads_schedd = collector9620.query(htcondor.AdTypes.Schedd, "true", projection_schedd)
dict_list_schedd= convert_ads_to_dict_list(ads_schedd)


amq=StompAMQ("","","monit_prod_cms_si_condor","/topic/cms.si.condor", [('dashb-test-mb.cern.ch', 61123)], logger=log, cert=my_cert, key=my_key, use_ssl=True)

timestamp=int(time.time())
notification=amq.make_notification(dict_list_schedd,"test_raw", "itb", "schedd", ts=timestamp)

 
failedNotifications=amq.send(notification)
for failed in failedNotifications:
    print("Failed")
    print(failed)





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
