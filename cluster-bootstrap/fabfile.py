#!/usr/bin/env python2
# encoding: utf-8

# Author: Alexandre Fonseca
# Description:
#   Installs, configures and manages Hadoop on a set of nodes
#   in a cluster.
# Associated guide:
#   http://www.alexjf.net/blog/distributed-systems/hadoop-yarn-installation-definitive-guide
from frontera import bootstrapFrontera, calcFronteraLayout, startSpiders, stopSpiders, startWorkers, stopWorkers
import frontera
from common import installDependencies, readHostsFromEC2
import common

import os
from fabric.api import run, cd, env, settings, put, sudo
from fabric.decorators import runs_once, parallel
from fabric.tasks import execute

###############################################################
#  START OF YOUR CONFIGURATION (CHANGE FROM HERE, IF NEEDED)  #
###############################################################

#### Generic ####
SSH_USER = "ubuntu"
# If you need to specify a special ssh key, do it here (e.g EC2 key)
#env.key_filename = "~/.ssh/giraph.pem"

# Read AWS access key details from env if available
AWS_ACCESSKEY_ID = os.getenv("AWS_ACCESSKEY_ID", "undefined")
AWS_ACCESSKEY_SECRET = os.getenv("AWS_ACCESSKEY_SECRET", "undefined")
# In case the instances you use have an extra storage device which is not
# automatically mounted, specify here the path to that device.
EC2_INSTANCE_STORAGEDEV = [("/dev/xvdb", "/mnt"), ("/dev/xvdc", "/mnt1"), ("/dev/xvdd", "/mnt2")]


#### Package Information ####
APPS_PREFIX = "/home/ubuntu/Programs"
JAVA_HOME = "/usr/lib/jvm/java-7-openjdk-amd64"
HADOOP_VERSION = "2.5.2"
HADOOP_PACKAGE = "hadoop-%s" % HADOOP_VERSION
#HADOOP_PACKAGE_URL = "http://apache.mirrors.spacedump.net/hadoop/common/stable/%s.tar.gz" % HADOOP_PACKAGE
HADOOP_PACKAGE_URL = "http://www.whoishostingthis.com/mirrors/apache/hadoop/common/%(hadoop)s/%(hadoop)s.tar.gz" % \
                     {'hadoop': HADOOP_PACKAGE}
HADOOP_PREFIX = "%s/%s" % (APPS_PREFIX, HADOOP_PACKAGE)
HADOOP_CONF = os.path.join(HADOOP_PREFIX, "etc/hadoop")

ZOOKEEPER_VERSION = "3.4.6"
ZOOKEEPER_PACKAGE = "zookeeper-%s" % ZOOKEEPER_VERSION
ZOOKEEPER_PACKAGE_URL = "http://www.whoishostingthis.com/mirrors/apache/zookeeper/zookeeper-%(zk)s/zookeeper-%(zk)s.tar.gz" % \
                        {'zk': ZOOKEEPER_VERSION}
ZOOKEEPER_PREFIX = "%s/%s" % (APPS_PREFIX, ZOOKEEPER_PACKAGE)

HBASE_VERSION = "1.0.1.1"
HBASE_PACKAGE = "hbase-%s" % (HBASE_VERSION)
HBASE_PACKAGE_URL = "http://www.whoishostingthis.com/mirrors/apache/hbase/%(ver)s/hbase-%(ver)s-bin.tar.gz" % \
                    {"ver": HBASE_VERSION}
HBASE_PREFIX = "%s/%s" % (APPS_PREFIX, HBASE_PACKAGE)
HBASE_CONF = "%s/conf" % (HBASE_PREFIX)
HBASE_ENVIRONMENT_FILE = "%s/hbase-env.sh" % HBASE_CONF
HBASE_ENVIRONMENT_VARIABLES = [
    ("JAVA_HOME", JAVA_HOME),
    ("HBASE_MANAGES_ZK", "false"),
    ("HBASE_HEAPSIZE", "8G",),
    ("HBASE_LIBRARY_PATH", HADOOP_PREFIX + "/lib/native")
]

KAFKA_VERSION = "0.8.2.1"
KAFKA_VERSION2 = "2.10"
KAFKA_PACKAGE = "kafka_%s-%s" % (KAFKA_VERSION2, KAFKA_VERSION)
KAFKA_PACKAGE_URL = "http://www.whoishostingthis.com/mirrors/apache/kafka/%(ver)s/kafka_%(ver2)s-%(ver)s.tgz" % \
                    {"ver": KAFKA_VERSION,
                     "ver2": KAFKA_VERSION2}
KAFKA_PREFIX = "%s/%s" % (APPS_PREFIX, KAFKA_PACKAGE)
KAFKA_CONF = "%s/config" %(KAFKA_PREFIX)

#### Environment ####
# Set this to True/False depending on whether or not ENVIRONMENT_FILE
# points to an environment file that is automatically loaded in a new
# shell session
#ENVIRONMENT_FILE_NOTAUTOLOADED = True
#ENVIRONMENT_FILE = "/home/ubuntu/.bashrc"
ENVIRONMENT_FILE_NOTAUTOLOADED = True
ENVIRONMENT_FILE = "/home/ubuntu/hadoop2_env.sh"


# Should the ENVIRONMENT_VARIABLES be applies to a clean (empty) environment
# file or should they simply be merged (only additions and updates) into the
# existing environment file? In any case, the previous version of the file
# will be backed up.
ENVIRONMENT_FILE_CLEAN = False
ENVIRONMENT_VARIABLES = [
    ("JAVA_HOME", JAVA_HOME), # Debian/Ubuntu 64 bits
    #("JAVA_HOME", "/usr/lib/jvm/java-7-openjdk"), # Arch Linux
    #("JAVA_HOME", "/usr/java/jdk1.7.0_51"), # CentOS
    ("HADOOP_PREFIX", HADOOP_PREFIX),
    ("HADOOP_HOME", r"\\$HADOOP_PREFIX"),
    ("HADOOP_COMMON_HOME", r"\\$HADOOP_PREFIX"),
    ("HADOOP_CONF_DIR", r"\\$HADOOP_PREFIX/etc/hadoop"),
    ("HADOOP_HDFS_HOME", r"\\$HADOOP_PREFIX"),
    ("HADOOP_MAPRED_HOME", r"\\$HADOOP_PREFIX"),
    ("HADOOP_YARN_HOME", r"\\$HADOOP_PREFIX"),
    ("HADOOP_PID_DIR", "/tmp/hadoop_%s" % HADOOP_VERSION),
    ("YARN_PID_DIR", r"\\$HADOOP_PID_DIR"),
    ("PATH", r"\\$HADOOP_PREFIX/bin:\\$PATH"),
]


#### Host data (for non-EC2 deployments) ####
HOSTS_FILE="/etc/hosts"
NET_INTERFACE="eth0"

#### Configuration ####
# Should the configuration options be applied to a clean (empty) configuration
# file or should they simply be merged (only additions and updates) into the
# existing environment file? In any case, the previous version of the file
# will be backed up.
CONFIGURATION_FILES_CLEAN = False

HADOOP_TEMP = "/mnt/hadoop/tmp"
HDFS_DATA_DIR = {}
HDFS_NAME_DIR = {}
ZK_DATA_DIR = "/var/lib/zookeeper"
KAFKA_LOG_DIRS = "/mnt/kafka"

IMPORTANT_DIRS = [HADOOP_TEMP, ZK_DATA_DIR, KAFKA_LOG_DIRS]

# Need to do this in a function so that we can rewrite the values when any
# of the hosts change in runtime (e.g. EC2 node discovery).
def _updateHadoopSiteValues():
    global CORE_SITE_VALUES, HDFS_SITE_VALUES, YARN_SITE_VALUES, MAPRED_SITE_VALUES, HBASE_SITE_VALUES

    CORE_SITE_VALUES = {
        "fs.defaultFS": "hdfs://%s/" % common.NAMENODE_HOST,
        "fs.s3n.awsAccessKeyId": AWS_ACCESSKEY_ID,
        "fs.s3n.awsSecretAccessKey": AWS_ACCESSKEY_SECRET,
        "hadoop.tmp.dir": HADOOP_TEMP
    }

    HDFS_SITE_VALUES = {
        "dfs.datanode.data.dir": ",".join(map(lambda p: "file://" + p, HDFS_DATA_DIR[env.host])),
        "dfs.namenode.name.dir": ",".join(map(lambda p: "file://" + p, HDFS_NAME_DIR[env.host])),
        "dfs.permissions": "false",
        "dfs.replication": "2",
        "dfs.client.read.shortcircuit": "true",
        "dfs.domain.socket.path": "/var/lib/hadoop-hdfs/dn_socket"
    }

    YARN_SITE_VALUES = {
        "yarn.resourcemanager.hostname": common.RESOURCEMANAGER_HOST,
        "yarn.scheduler.minimum-allocation-mb": 128,
        "yarn.scheduler.maximum-allocation-mb": 1024,
        "yarn.scheduler.minimum-allocation-vcores": 1,
        "yarn.scheduler.maximum-allocation-vcores": 2,
        "yarn.nodemanager.resource.memory-mb": 4096,
        "yarn.nodemanager.resource.cpu-vcores": 4,
        "yarn.log-aggregation-enable": "true",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle",
        "yarn.nodemanager.vmem-pmem-ratio": 3.1,
        "yarn.nodemanager.remote-app-log-dir": os.path.join(HADOOP_TEMP, "logs"),
        "yarn.nodemanager.log-dirs": os.path.join(HADOOP_TEMP, "userlogs"),
    }

    MAPRED_SITE_VALUES = {
        "yarn.app.mapreduce.am.resource.mb": 1024,
        "yarn.app.mapreduce.am.command-opts": "-Xmx768m",
        "mapreduce.framework.name": "yarn",
        "mapreduce.map.cpu.vcores": 1,
        "mapreduce.map.memory.mb": 1024,
        "mapreduce.map.java.opts": "-Xmx768m",
        "mapreduce.reduce.cpu.vcores": 1,
        "mapreduce.reduce.memory.mb": 1024,
        "mapreduce.reduce.java.opts": "-Xmx768m",
    }

    HBASE_SITE_VALUES = {
        "hbase.cluster.distributed": "true",
        "hbase.rootdir": "hdfs://%s:8020/" % common.NAMENODE_HOST,
        "hbase.regionserver.thrift.compact": "true",
        "hbase.regionserver.thrift.framed": "true",
        "dfs.client.read.shortcircuit": "true",
        "dfs.domain.socket.path": "/var/lib/hadoop-hdfs/dn_socket"
    }

##############################################################
#  END OF YOUR CONFIGURATION (CHANGE UNTIL HERE, IF NEEDED)  #
##############################################################

#####################################################################
#  DON'T CHANGE ANYTHING BELOW (UNLESS YOU KNOW WHAT YOU'RE DOING)  #
#####################################################################
CORE_SITE_VALUES = {}
HDFS_SITE_VALUES = {}
YARN_SITE_VALUES = {}
MAPRED_SITE_VALUES = {}
HBASE_SITE_VALUES = {}

def bootstrapFabric():
    if common.EC2:
        common._load_ec2_data()
        readHostsFromEC2()

    env.user = SSH_USER
    hosts = [common.NAMENODE_HOST, common.RESOURCEMANAGER_HOST, common.JOBHISTORY_HOST] + common.SLAVE_HOSTS + \
            common.KAFKA_HOSTS + common.ZK_HOSTS + [common.HBASE_MASTER] + common.HBASE_RS + \
            common.HOSTS["frontera_spiders"] + common.HOSTS["frontera_workers"]
    seen = set()
    # Remove empty hosts and duplicates
    cleanedHosts = [host for host in hosts if host and host not in seen and not seen.add(host)]
    env.hosts = cleanedHosts

    if common.JOBTRACKER_HOST:
        MAPRED_SITE_VALUES["mapreduce.jobtracker.address"] = "%s:%s" % \
            (common.JOBTRACKER_HOST, common.JOBTRACKER_PORT)

    if common.JOBHISTORY_HOST:
        MAPRED_SITE_VALUES["mapreduce.jobhistory.address"] = "%s:%s" % \
            (common.JOBHISTORY_HOST, common.JOBHISTORY_PORT)

    calcFronteraLayout()
    _configureHDFSDirectories()

def _configureHDFSDirectories():
    hadoop_hosts = set(common.SLAVE_HOSTS + [common.RESOURCEMANAGER_HOST, common.NAMENODE_HOST, common.JOBTRACKER_HOST])
    for host in hadoop_hosts:
        type = common.INSTANCES[host].instance_type
        info = common.EC2_INSTANCE_DATA[type]
        for _, mount_point in EC2_INSTANCE_STORAGEDEV[:info['disks_count']]:
            data_dir = "%s/hdfs/datanode" % mount_point
            HDFS_DATA_DIR.setdefault(host, []).append(data_dir)

            name_dir = "%s/hdfs/namenode" % mount_point
            HDFS_NAME_DIR.setdefault(host, []).append(name_dir)

# MAIN FUNCTIONS
def forceStopEveryJava():
    run("jps | grep -vi jps | cut -d ' ' -f 1 | xargs -L1 -r kill")


@runs_once
def debugHosts():
    print("Resource Manager: {}".format(common.RESOURCEMANAGER_HOST))
    print("Name node: {}".format(common.NAMENODE_HOST))
    print("Job Tracker: {}".format(common.JOBTRACKER_HOST))
    print("Job History: {}".format(common.JOBHISTORY_HOST))
    print("Slaves: {}".format(common.SLAVE_HOSTS))
    print("ZK: {}".format(common.ZK_HOSTS))
    print("HBase Master: {}".format(common.HBASE_MASTER))
    print("HBase Region Servers: {}".format(common.HBASE_RS))
    print("Kafka: {}".format(common.KAFKA_HOSTS))
    print("Spiders: {}".format(common.HOSTS["frontera_spiders"]))
    print("Frontera workers: {}".format(common.HOSTS["frontera_workers"]))
    print("Frontera cluster config:")
    print frontera.FRONTERA_CLUSTER_CONFIG

def _prepareStorageDevices():
    with settings(warn_only=True):
        type = common.INSTANCES[env.host].instance_type
        info = common.EC2_INSTANCE_DATA[type]
        for device, mount_point in EC2_INSTANCE_STORAGEDEV[:info['disks_count']]:
            sudo("umount %s" % mount_point)
            sudo("mkfs.ext4 %s" % device)
            sudo("mount -o defaults,noatime %s %s" % (device, mount_point))
            sudo("chmod 0777 %s" % mount_point)
        sudo("rm -rf /tmp/hadoop-ubuntu")

def bootstrap():
    _prepareStorageDevices()
    ensureImportantDirectoriesExist()

    installDependencies()
    installHadoop()
    installZookeeper()
    installHBase()
    installKafka()

    if common.isService('hadoop') or common.isService('hbase'):
        _updateHadoopSiteValues()
        setupEnvironment()
        configHadoop()

    configZookeeper()
    configHBase()
    configKafka()
    setupHosts()

    formatHdfs()
    cleanup()

def postBootstrap():
    #cleanupHBaseZookeeper()
    deleteFronteraKafkaTopics()
    createFronteraKafkaTopics()


@runs_once
def createHBaseNamespace(namespace):
    if env.host != common.HBASE_MASTER:
        return

    fh = open("hbasecmd.txt", "w")
    print >> fh, "create_namespace '%s'" % namespace
    print >> fh, "quit"
    fh.close()
    put("hbasecmd.txt", HBASE_PREFIX)
    with cd(HBASE_PREFIX):
        run("bin/hbase shell < hbasecmd.txt")
        run("rm -f hbasecmd.txt")
    os.remove("hbasecmd.txt")


@runs_once
def cleanupHBaseZookeeper():
    if env.host not in common.ZK_HOSTS:
        return

    fh = open("zkcmds.txt", "w")
    print >> fh, "rmr /hbase"
    print >> fh, "quit"
    fh.close()
    put("zkcmds.txt", ZOOKEEPER_PREFIX)
    with cd(ZOOKEEPER_PREFIX):
        run("bin/zkCli.sh < zkcmds.txt")
        run("rm -f zkcmds.txt")
    os.remove("zkcmds.txt")

def ensureImportantDirectoriesExist():
    for importantDir in IMPORTANT_DIRS:
        ensureDirectoryExists(importantDir)

    if common.isService('hadoop'):
        for path in HDFS_DATA_DIR[env.host]+HDFS_NAME_DIR[env.host]:
            ensureDirectoryExists(path)
        ensureDirectoryExists('/var/lib/hadoop-hdfs')

def installHadoop():
    installDirectory = os.path.dirname(HADOOP_PREFIX)
    run("mkdir -p %s" % installDirectory)
    with cd(installDirectory):
        with settings(warn_only=True):
            if run("test -f %s.tar.gz" % HADOOP_PACKAGE).failed:
                run("wget -O %s.tar.gz %s" % (HADOOP_PACKAGE, HADOOP_PACKAGE_URL))
        run("tar --overwrite -xf %s.tar.gz" % HADOOP_PACKAGE)

def installZookeeper():
    if env.host not in common.ZK_HOSTS:
        return
    installDirectory = os.path.dirname(ZOOKEEPER_PREFIX)
    run("mkdir -p %s" % installDirectory)
    with cd(installDirectory):
        with settings(warn_only=True):
            if run("test -f %s.tar.gz" % ZOOKEEPER_PACKAGE).failed:
                run("wget -O %s.tar.gz %s" % (ZOOKEEPER_PACKAGE, ZOOKEEPER_PACKAGE_URL))
        run("tar --overwrite -xf %s.tar.gz" % ZOOKEEPER_PACKAGE)

def installHBase():
    if env.host not in common.HBASE_RS and env.host != common.HBASE_MASTER:
        return
    installDirectory = os.path.dirname(HBASE_PREFIX)
    run("mkdir -p %s" % installDirectory)
    with cd(installDirectory):
        with settings(warn_only=True):
            if run("test -f %s-bin.tar.gz" % HBASE_PACKAGE).failed:
                run("wget -O %s-bin.tar.gz %s" % (HBASE_PACKAGE, HBASE_PACKAGE_URL))
        run("tar --overwrite -xf %s-bin.tar.gz" % HBASE_PACKAGE)

def installKafka():
    if env.host not in common.KAFKA_HOSTS:
        return
    installDirectory = os.path.dirname(KAFKA_PREFIX)
    run("mkdir -p %s" % installDirectory)
    with cd(installDirectory):
        with settings(warn_only=True):
            if run("test -f %s.tar.gz" % KAFKA_PACKAGE).failed:
                run("wget -O %s.tar.gz %s" % (KAFKA_PACKAGE, KAFKA_PACKAGE_URL))
        run("tar --overwrite -xf %s.tar.gz" % KAFKA_PACKAGE)

def configHadoop():
    changeHadoopProperties(HADOOP_CONF, "core-site.xml", CORE_SITE_VALUES)
    changeHadoopProperties(HADOOP_CONF, "hdfs-site.xml", HDFS_SITE_VALUES)
    changeHadoopProperties(HADOOP_CONF, "yarn-site.xml", YARN_SITE_VALUES)
    changeHadoopProperties(HADOOP_CONF, "mapred-site.xml", MAPRED_SITE_VALUES)

def configHBase():
    if env.host not in common.HBASE_RS and env.host != common.HBASE_MASTER:
        return
    changeHadoopProperties(HBASE_CONF, "hbase-site.xml", HBASE_SITE_VALUES)

    fh = open("regionservers", "w")
    for host in common.HBASE_RS:
        print >> fh, host
    fh.close()
    put("regionservers", HBASE_CONF + "/")

def configZookeeper():
    if env.host not in common.ZK_HOSTS:
        return
    fh = open("zoo.cfg", "w")
    print >> fh, "tickTime=2000"
    print >> fh, "clientPort=2181"
    print >> fh, "dataDir=%s" % (ZK_DATA_DIR)
    print >> fh, "initLimit=5"
    print >> fh, "syncLimit=2"
    id = 1
    for host in common.ZK_HOSTS:
        print >> fh, "server.%d=%s:2888:3888" % (id, host)
        id += 1
    fh.close()
    put("zoo.cfg", ZOOKEEPER_PREFIX + "/conf/")

    fh = open("myid", "w")
    print >> fh, "%d" % (common.ZK_HOSTS.index(env.host) + 1)
    fh.close()
    put("myid", ZK_DATA_DIR + "/")

def configKafka():
    if env.host not in common.KAFKA_HOSTS:
        return

    fh = open("config-templates/kafka-server.properties")
    template = fh.read()
    fh.close()

    index = common.KAFKA_HOSTS.index(env.host)
    fh = open("server-generated.properties", "w")
    config = template.format(broker_id=str(index), log_dirs=KAFKA_LOG_DIRS)
    fh.write(config)
    fh.close()
    put("server-generated.properties", KAFKA_CONF + "/")

def configRevertPrevious():
    revertHadoopPropertiesChange("core-site.xml")
    revertHadoopPropertiesChange("hdfs-site.xml")
    revertHadoopPropertiesChange("yarn-site.xml")
    revertHadoopPropertiesChange("mapred-site.xml")

def setupEnvironment():
    updateEnvironment(ENVIRONMENT_FILE, ENVIRONMENT_VARIABLES, ENVIRONMENT_FILE_CLEAN)

    if env.host in common.HBASE_RS or env.host == common.HBASE_MASTER:
        updateEnvironment(HBASE_ENVIRONMENT_FILE, HBASE_ENVIRONMENT_VARIABLES, ENVIRONMENT_FILE_CLEAN)

def updateEnvironment(filename, vars, is_clean):
    with settings(warn_only=True):
        if not run("test -f %s" % filename).failed:
            op = "cp"

            if is_clean:
                op = "mv"

            currentBakNumber = getLastBackupNumber(filename) + 1
            run("%(op)s %(file)s %(file)s.bak%(bakNumber)d" %
                {"op": op, "file": filename, "bakNumber": currentBakNumber})

    run("touch %s" % filename)

    for variable, value in vars:
        lineNumber = run("grep -n 'export\s\+%(var)s\=' '%(file)s' | cut -d : -f 1" %
                {"var": variable, "file": filename})
        try:
            lineNumber = int(lineNumber)
            run("sed -i \"" + str(lineNumber) + "s@.*@export %(var)s\=%(val)s@\" '%(file)s'" %
                {"var": variable, "val": value, "file": filename})
        except ValueError:
            run("echo \"export %(var)s=%(val)s\" >> \"%(file)s\"" %
                {"var": variable, "val": value, "file": filename})


def environmentRevertPrevious():
    revertBackup(ENVIRONMENT_FILE)


def formatHdfs():
    if env.host == common.NAMENODE_HOST:
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/bin/hdfs namenode -format")


@runs_once
def setupHosts():
    privateIps = execute(getPrivateIp)
    execute(updateHosts, privateIps)

    if env.host == common.RESOURCEMANAGER_HOST:
        run("rm -f privateIps")
        run("touch privateIps")

        for host, privateIp in privateIps.items():
            run("echo '%s' >> privateIps" % privateIp)

@runs_once
def deleteFronteraKafkaTopics():
    if env.host not in common.KAFKA_HOSTS:
        return
    with cd(KAFKA_PREFIX):
        for topic in ['frontier-done', 'frontier-todo', 'frontier-score', 'hh-results', 'hh-incoming']:
            run("bin/kafka-topics.sh --delete --topic %s --zookeeper %s" % (topic, common.ZK_HOSTS[0]))

@runs_once
def createFronteraKafkaTopics():
    if env.host not in common.KAFKA_HOSTS:
        return
    with cd(KAFKA_PREFIX):
        spider_count = frontera.FRONTERA_CLUSTER_CONFIG['spider_instances']
        sw_count = frontera.FRONTERA_CLUSTER_CONFIG['sw_instances']
        run("bin/kafka-topics.sh --create --topic frontier-done --replication-factor 1 --partitions %d --zookeeper %s:2181" %
            (sw_count, common.ZK_HOSTS[0]))
        run("bin/kafka-topics.sh --create --topic frontier-todo --replication-factor 1 --partitions %d --zookeeper %s:2181" %
            (spider_count, common.ZK_HOSTS[0]))
        run("bin/kafka-topics.sh --create --topic frontier-score --replication-factor 1 --partitions %d --zookeeper %s:2181" %
            (sw_count, common.ZK_HOSTS[0]))
        run("bin/kafka-topics.sh --create --topic hh-incoming --replication-factor 1 --partitions 1 --zookeeper %s:2181" %
            (common.ZK_HOSTS[0]))
        run("bin/kafka-topics.sh --create --topic hh-results --replication-factor 1 --partitions 1 --zookeeper %s:2181" %
            (common.ZK_HOSTS[0]))

def startZookeeper():
    operationOnZookeeperDaemon("start")

def stopZookeeper():
    operationOnZookeeperDaemon("stop")

def operationOnZookeeperDaemon(command):
    if env.host not in common.ZK_HOSTS:
        return
    with cd(ZOOKEEPER_PREFIX):
        run("bin/zkServer.sh %s" % command)

def startKafka():
    if env.host not in common.KAFKA_HOSTS:
        return
    with cd(KAFKA_PREFIX):
        run("nohup bin/kafka-server-start.sh -daemon config/server-generated.properties")

def stopKafka():
    if env.host not in common.KAFKA_HOSTS:
        return
    with cd(KAFKA_PREFIX):
        run("bin/kafka-server-stop.sh")

def startHadoop():
    operationOnHadoopDaemons("start")

def stopHadoop():
    operationOnHadoopDaemons("stop")


def operationOnHBase(command, args):
    with cd(HBASE_PREFIX):
        run("bin/hbase-daemon.sh --config %s %s %s" % (HBASE_CONF, command, args))

def startHBase():
    if env.host == common.HBASE_MASTER:
        operationOnHBase("start", "master")

    if env.host in common.HBASE_RS:
        operationOnHBase("start", "regionserver")
        operationOnHBase("start", "thrift")

def stopHBase():
    if env.host == common.HBASE_MASTER:
        operationOnHBase("stop", "master")

    if env.host in common.HBASE_RS:
        operationOnHBase("stop", "thrift")
        operationOnHBase("stop", "regionserver")


def testHadoop():
    if env.host == common.RESOURCEMANAGER_HOST:
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/bin/hadoop jar \\$HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-%(version)s.jar org.apache.hadoop.yarn.applications.distributedshell.Client --jar \\$HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-%(version)s.jar --shell_command date --num_containers %(numContainers)d --master_memory 1024" %
            {"version": HADOOP_VERSION, "numContainers": len(common.SLAVE_HOSTS)})


def testMapReduce():
    if env.host == common.RESOURCEMANAGER_HOST:
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/bin/hadoop dfs -rm -f -r out")
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/bin/hadoop jar \\$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-%s.jar randomwriter out" % HADOOP_VERSION)


# HELPER FUNCTIONS
def ensureDirectoryExists(directory):
    with settings(warn_only=True):
        if run("test -d %s" % directory).failed:
            sudo("mkdir -p %s" % directory)
            sudo("chown %(user)s:%(user)s %(dir)s" % {"user": SSH_USER, "dir": directory})


@parallel
def getPrivateIp():
    if not common.EC2:
        return run("ifconfig %s | grep 'inet\s\+' | awk '{print $2}' | cut -d':' -f2" % NET_INTERFACE).strip()
    else:
        return run("wget -qO- http://instance-data/latest/meta-data/local-ipv4")


@parallel
def updateHosts(privateIps):
    with settings(warn_only=True):
        if not run("test -f %s" % HOSTS_FILE).failed:
            currentBakNumber = getLastBackupNumber(HOSTS_FILE) + 1
            sudo("cp %(file)s %(file)s.bak%(bakNumber)d" %
                {"file": HOSTS_FILE, "bakNumber": currentBakNumber})

    sudo("touch %s" % HOSTS_FILE)

    for host, privateIp in privateIps.items():
        lineNumber = run("grep -n -F -w -m 1 '%(ip)s' '%(file)s' | cut -d : -f 1" %
                {"ip": privateIp, "file": HOSTS_FILE})
        try:
            lineNumber = int(lineNumber)
            sudo("sed -i \"" + str(lineNumber) + "s@.*@%(ip)s %(host)s@\" '%(file)s'" %
                {"host": host, "ip": privateIp, "file": HOSTS_FILE})
        except ValueError:
            sudo("echo \"%(ip)s %(host)s\" >> \"%(file)s\"" %
                {"host": host, "ip": privateIp, "file": HOSTS_FILE})


def getLastBackupNumber(filePath):
    dirName = os.path.dirname(filePath)
    fileName = os.path.basename(filePath)

    with cd(dirName):
        latestBak = run("ls -1 | grep %s.bak | tail -n 1" % fileName)
        latestBakNumber = -1
        if latestBak:
            latestBakNumber = int(latestBak[len(fileName) + 4:])
        return latestBakNumber


def changeHadoopProperties(path, fileName, propertyDict):
    if not fileName or not propertyDict:
        return

    with cd(path):
        with settings(warn_only=True):
            import hashlib
            replaceHadoopPropertyHash = \
                hashlib.md5(
                    open("replaceHadoopProperty.py", 'rb').read()
                ).hexdigest()
            if run("test %s = `md5sum replaceHadoopProperty.py | cut -d ' ' -f 1`"
                   % replaceHadoopPropertyHash).failed:
                put("replaceHadoopProperty.py", path + "/")
                run("chmod +x replaceHadoopProperty.py")

        with settings(warn_only=True):
            if not run("test -f %s" % fileName).failed:
                op = "cp"

                if CONFIGURATION_FILES_CLEAN:
                    op = "mv"

                currentBakNumber = getLastBackupNumber(fileName) + 1
                run("%(op)s %(file)s %(file)s.bak%(bakNumber)d" %
                    {"op": op, "file": fileName, "bakNumber": currentBakNumber})

        run("touch %s" % fileName)

        command = "./replaceHadoopProperty.py '%s' %s" % (fileName,
            " ".join(["%s %s" % (str(key), str(value)) for key, value in propertyDict.items()]))
        run(command)


def revertBackup(fileName):
    dirName = os.path.dirname(fileName)

    with cd(dirName):
        latestBakNumber = getLastBackupNumber(fileName)

        # We have already reverted all backups
        if latestBakNumber == -1:
            return
        # Otherwise, perform reversion
        else:
            run("mv %(file)s.bak%(bakNumber)d %(file)s" %
                {"file": fileName, "bakNumber": latestBakNumber})


def revertHadoopPropertiesChange(fileName):
    revertBackup(os.path.join(HADOOP_CONF, fileName))


def operationInHadoopEnvironment(operation):
    with cd(HADOOP_PREFIX):
        command = operation
        if ENVIRONMENT_FILE_NOTAUTOLOADED:
            with settings(warn_only=True):
                import hashlib
                executeInHadoopEnvHash = \
                    hashlib.md5(
                        open("executeInHadoopEnv.sh", 'rb').read()
                    ).hexdigest()
                if run("test %s = `md5sum executeInHadoopEnv.sh | cut -d ' ' -f 1`"
                    % executeInHadoopEnvHash).failed:
                    put("executeInHadoopEnv.sh", HADOOP_PREFIX + "/")
                    run("chmod +x executeInHadoopEnv.sh")
            command = ("./executeInHadoopEnv.sh %s " % ENVIRONMENT_FILE) + command
        run(command)

def operationOnHadoopDaemons(operation):
    # Start/Stop NameNode
    if (env.host == common.NAMENODE_HOST):
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/sbin/hadoop-daemon.sh %s namenode" % operation)

    # Start/Stop DataNode on all slave hosts
    if env.host in common.SLAVE_HOSTS:
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/sbin/hadoop-daemon.sh %s datanode" % operation)

    # Start/Stop ResourceManager
    if (env.host == common.RESOURCEMANAGER_HOST):
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/sbin/yarn-daemon.sh %s resourcemanager" % operation)

    # Start/Stop NodeManager on all container hosts
    if env.host in common.SLAVE_HOSTS:
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/sbin/yarn-daemon.sh %s nodemanager" % operation)

    # Start/Stop JobHistory daemon
    if (env.host == common.JOBHISTORY_HOST):
        operationInHadoopEnvironment(r"\\$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh %s historyserver" % operation)
    run("jps")

@runs_once
def cleanup():
    for fname in ["server-generated.properties", "regionservers", "myid", "zoo.cfg"]:
        os.remove(fname)

bootstrapFabric()
