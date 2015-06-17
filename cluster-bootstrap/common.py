# -*- coding: utf-8 -*-
from fabric.api import sudo, env
import json
#### EC2 ####
# Is this an EC2 deployment? If so, then we'll autodiscover the right nodes.
EC2 = True
EC2_REGION = "us-west-2"
# In case this is an EC2 deployment, all cluster nodes must have a tag with
# 'Cluster' as key and the following property as value.
EC2_CLUSTER_NAME = "frontera"
# Should ResourceManager participate in job execution (also be a slave node?)
EC2_RM_NONSLAVE = True


#### Hosts ####
RESOURCEMANAGER_HOST = "frontera-master"
NAMENODE_HOST = RESOURCEMANAGER_HOST

SLAVE_HOSTS = ["frontera%d" % i for i in range(1, 2)]
ZK_HOSTS = []
HBASE_MASTER = None
HBASE_RS = []
KAFKA_HOSTS = []
HOSTS = {
    "frontera_workers": [],
    "frontera_spiders": []
}
INSTANCES = {}
EC2_INSTANCE_DATA = {}


# If you'll be running map reduce jobs, you should choose a host to be
# the job tracker
JOBTRACKER_HOST = SLAVE_HOSTS[0]
JOBTRACKER_PORT = 8021

# If you'll run MapReduce jobs, you might want to set a JobHistory server.
# e.g: JOBHISTORY_HOST = "jobhistory.alexjf.net"
JOBHISTORY_HOST = JOBTRACKER_HOST
JOBHISTORY_PORT = 10020


#### Installation information ####
# Change this to the command you would use to install packages on the
# remote hosts.
PACKAGE_MANAGER_INSTALL = "apt-get -qq install %s" # Debian/Ubuntu
#PACKAGE_MANAGER_INSTALL = "pacman -S %s" # Arch Linux
#PACKAGE_MANAGER_INSTALL = "yum install %s" # CentOS

# Change this list to the list of packages required by Hadoop
# In principle, should just be a JRE for Hadoop, Python
# for the Hadoop Configuration replacement script and wget
# to get the Hadoop package
REQUIREMENTS = ["wget", "python", "openjdk-7-jdk", "libsnappy1"] # Debian/Ubuntu
#REQUIREMENTS = ["wget", "python", "openjdk-7-jre-headless"] # Debian/Ubuntu
#REQUIREMENTS = ["wget", "python", "jre7-openjdk-headless"] # Arch Linux
#REQUIREMENTS = ["wget", "python", "java-1.7.0-openjdk-devel"] # CentOS

# Commands to execute (in order) before installing listed requirements
# (will run as root). Use to configure extra repos or update repos
# If you want to install Oracle's Java instead of using the OpenJDK that
# comes preinstalled with most distributions replace the previous options
# with a variation of the following: (UBUNTU only)
#REQUIREMENTS = ["wget", "python", "oracle-java7-installer"] # Debian/Ubuntu
REQUIREMENTS_PRE_COMMANDS = [
    #"add-apt-repository ppa:webupd8team/java -y",
    "apt-get -qq update",
    #"echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections",
    #"echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections",
]

def installDependencies(requirements=None, pre_commands=True):
    if pre_commands:
        for command in REQUIREMENTS_PRE_COMMANDS:
            sudo(command)

    r = requirements if requirements else REQUIREMENTS
    sudo(PACKAGE_MANAGER_INSTALL % str(" ").join(r))


def readHostsFromEC2():
    import boto.ec2

    global RESOURCEMANAGER_HOST, NAMENODE_HOST, JOBTRACKER_HOST, KAFKA_HOSTS, \
        JOBHISTORY_HOST, SLAVE_HOSTS, ZK_HOSTS, HBASE_MASTER, HBASE_RS, SPIDERS_HOSTS, HOSTS, INSTANCES

    RESOURCEMANAGER_HOST = None
    NAMENODE_HOST = None
    JOBTRACKER_HOST = None
    JOBHISTORY_HOST = None
    SLAVE_HOSTS = []
    ZK_HOSTS = []
    SPIDERS_HOSTS = []
    HOSTS= {'frontera_workers': [], 'frontera_spiders': []}
    INSTANCES = {}

    conn = boto.ec2.connect_to_region(EC2_REGION)
    instances = conn.get_only_instances(filters={'tag:Cluster': EC2_CLUSTER_NAME})

    for instance in instances:
        instanceTags = instance.tags
        instanceHost = instance.public_dns_name
        if not instanceHost:
            continue

        INSTANCES[instanceHost] = instance

        if "resourcemanager" in instanceTags:
            RESOURCEMANAGER_HOST = instanceHost

        if "namenode" in instanceTags:
            NAMENODE_HOST = instanceHost

        if "jobhistory" in instanceTags:
            JOBHISTORY_HOST = instanceHost

        if "jobtracker" in instanceTags:
            JOBTRACKER_HOST = instanceHost

        if "zk" in instanceTags:
            ZK_HOSTS.append(instanceHost)

        if "hbase-master" in instanceTags:
            HBASE_MASTER = instanceHost

        if "hbase-rs" in instanceTags:
            HBASE_RS.append(instanceHost)

        if "kafka" in instanceTags:
            KAFKA_HOSTS.append(instanceHost)

        if "spiders" in instanceTags:
            HOSTS["frontera_spiders"].append(instanceHost)
            continue

        if "workers" in instanceTags:
            HOSTS["frontera_workers"].append(instanceHost)
            continue

        if not EC2_RM_NONSLAVE or instanceHost != RESOURCEMANAGER_HOST:
            SLAVE_HOSTS.append(instanceHost)


    if SLAVE_HOSTS:
        if RESOURCEMANAGER_HOST is None:
            RESOURCEMANAGER_HOST = SLAVE_HOSTS[0]

            if EC2_RM_NONSLAVE:
                SLAVE_HOSTS.remove(0)

        if NAMENODE_HOST is None:
            NAMENODE_HOST = RESOURCEMANAGER_HOST

        if JOBTRACKER_HOST is None:
            JOBTRACKER_HOST = SLAVE_HOSTS[0]

        if JOBHISTORY_HOST is None:
            JOBHISTORY_HOST = SLAVE_HOSTS[0]

def _load_ec2_data():
    # To update this table, clone this repo:
    # https://github.com/powdahound/ec2instances.info
    # and run 'fab build' command.
    raw_data = json.load(open("instances.json", "r"))
    for r in raw_data:
        disks = (r.get("storage") or {}).get("devices", 0)

        EC2_INSTANCE_DATA[r["instance_type"]] = {
            "instance_type": r["instance_type"],
            "cpucores": r["vCPU"],
            "ram": r["memory"],
            "disks_count": disks
        }

def isService(service):
    if service=='hadoop':
        if env.host in SLAVE_HOSTS or env.host in [NAMENODE_HOST, RESOURCEMANAGER_HOST, JOBTRACKER_HOST]:
            return True
        return False
    if service=='hbase':
        if env.host in HBASE_RS or env.host == HBASE_MASTER:
            return True
        return False
    raise NotImplementedError('Unknown service')