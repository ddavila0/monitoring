# monitoring
This is the repository for the code which supplies the monitoring metrics for the HTCondor infrastructure of CMS. 
It is owned and mantained by the Submission Infrastructure group.

# list of attributes
There are, at the moment, 5 categories of data that we are curently providing: 
1. Autocluster
2. Collector
3. Negotiator
4. Schedd. 
5. Startd

Each one of the components listed above have its own set of attributes. In the following sections you can see a short description
of these attrbutes. For more information, please read the "ClassAd Attributes" section of the HTCondor manual:
https://research.cs.wisc.edu/htcondor/manual/latest

## Autocluster
In HTCondor an autocluster is a greoup of jobs in one Schedd that share similar attribute values. This grouping is one, by default,
automatically by condor, this means that the set of attributes used to group the jobs changes dynamically.

AcctGroup. Accounting group of the jobs, it tells to wchich group a job belongs e.g. analysis, tier0, production
AutoClusterId. Unique identifier of the autocluster within the schedd
CMS_JobType. The type of job according to CMS e.g. Analysis, Production, processing, Repack, Merge, etc
CRAB_ReqName. 
DiskUsage. Amount of disk space (KiB) in the HTCondor execute directory on the execute machine that this grouip of jobs have used
JobCpus. 
JobPrio
JobStatus
MATCH_GLIDEIN_CMSSite
MaxCores
MemoryUsage
MinCores
OriginalCpus
RemoteUserCpu
RemoteWallClockTime
RequestCPUs
RequestDisk
RequestMemory
ResidentSetSize
WMAgent_RequestName
WMAgent_SubTaskName

## Collector
ActiveQueryWorkers
PendingQueries
RecentDaemonCoreDutyCycle
RecentDroppedQueries
RecentForkQueriesFromNEGOTIATOR
RecentForkQueriesFromTOOL
RecentUpdatesLost
RecentUpdatesTotal
SubmitterAds

## Negotiator
LastNegotiationCycleActiveSubmitterCount0,
LastNegotiationCycleNumIdleJobs0
LastNegotiationCyclePhase1Duration0
LastNegotiationCyclePhase2Duration0
LastNegotiationCyclePhase3Duration0
LastNegotiationCyclePhase4Duration0
LastNegotiationCycleScheddsOutOfTime0
LastNegotiationCycleTotalSlots0,
MonitorSelfCPUUsage
MyCurrentTime
MyType
Name

## Schedd
Autoclusters
CMSGWMS_Type
MaxJobsRunning
MyType
Name
NumOwners
RecentDaemonCoreDutyCycle
RecentJobsCompleted
RecentJobsStarted
RecentJobsSubmitted
RecentResourceRequestsSent
ServerTime
TotalHeldJobs
TotalIdleJobs
TotalRunningJobs

## Startd
Activity
CPUs
DaemonStartTime
DetectedIoslots
DetectedRepackslots
Disk
GLIDECLIENT_Group
GLIDECLIENT_Name
GLIDEIN_ClusterId
GLIDEIN_CMSSite
GLIDEIN_Entry_Name
GLIDEIN_Factory
GLIDEIN_PS_HAS_SINGULARITY
GLIDEIN_Job_Max_Time
GLIDEIN_MAX_Walltime
GLIDEIN_ProcId
GLIDEIN_REQUIRED_OS
GLIDEIN_Schedd
GLIDEIN_ToDie
GLIDEIN_ToRetire
GlobalJobId
Ioslots
IOSlots
Memory
MyCurrentTime
MyType
Repackslots
RepackSlots
SlotType
State
TotalIOSlots
TotalRepackSlots
TotalSlotCpus
TotalSlotMemory





