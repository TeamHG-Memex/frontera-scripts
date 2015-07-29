# -*- coding: utf-8 -*-
import os, os.path
import shutil
from common import installDependencies
import common
import json
from fabric.api import run, cd, env, settings, put, sudo
from fabric.decorators import runs_once, parallel
from fabric.tasks import execute

FRONTERA_TAG = "v.1"
FRONTERA_DEST_DIR = "/home/ubuntu/frontera"
FRONTERA_CRAWLER_DEST_DIR = "/home/ubuntu/frontera-crawler"
FRONTERA_SPIDER_DIR = "/home/ubuntu/topical-spiders"
FRONTERA_SETTINGS_DIR  = FRONTERA_SPIDER_DIR + "/frontier/"

FRONTERA_SPIDER_REPO = "https://github.com/TeamHG-Memex/topical-spiders.git"
FRONTERA_SPIDER_BUNDLE = "topical-spiders.tar.gz"
FRONTERA_SPIDER_DIRNAME = os.path.basename(FRONTERA_SPIDER_DIR)
FRONTERA_CRAWLER_BUNDLE = "frontera-crawler.tar.gz"
FRONTERA_CRAWLER_DIRNAME = os.path.basename(FRONTERA_CRAWLER_DEST_DIR)
FRONTERA_CRAWLER_REPO = "https://github.com/TeamHG-Memex/frontera-crawler.git"
FRONTERA_CLUSTER_CONFIG = {}


def setupDnsmasq():
    fh = open("resolv.dnsmasq.conf", "w")
    print >> fh, """
# Verizon
nameserver 199.45.32.37
nameserver 199.45.32.38
nameserver 199.45.32.40
nameserver 199.45.32.43

# OpenDNS
nameserver 208.67.222.222
nameserver 208.67.220.220

options rotate
"""
    fh.close()

    fh = open("dnsmasq.conf", "w")
    print >> fh, """
resolv-file=/etc/resolv.dnsmasq.conf
interface=lo
no-dhcp-interface=lo
"""
    fh.close()
    put("resolv.dnsmasq.conf", "/etc/resolv.dnsmasq.conf", use_sudo=True)
    put("dnsmasq.conf", "/etc/dnsmasq.conf", use_sudo=True)
    sudo("service dnsmasq restart")
    os.remove("dnsmasq.conf")
    os.remove("resolv.dnsmasq.conf")


def cloneFrontera():
    run("rm -rf %s" % FRONTERA_DEST_DIR)
    run("git clone -q https://github.com/scrapinghub/frontera.git %s" % FRONTERA_DEST_DIR)
    with cd(FRONTERA_DEST_DIR):
        # run("git checkout -q %s" % FRONTERA_TAG)
        run("git checkout distributed-hgmemex")

    # adding Frontera to python module path
    python_path = "/usr/lib/python2.7/dist-packages/ubuntu.pth"
    sudo("rm -f %s" % python_path)
    fh = open("ubuntu.pth", "w")
    print >> fh, FRONTERA_DEST_DIR
    print >> fh, FRONTERA_CRAWLER_DEST_DIR
    fh.close()
    put("ubuntu.pth", python_path, use_sudo=True)
    os.remove("ubuntu.pth")

def deploySpiders():
    put(FRONTERA_SPIDER_BUNDLE)
    put(FRONTERA_CRAWLER_BUNDLE)
    fc_name = os.path.basename(FRONTERA_CRAWLER_BUNDLE)
    fname = os.path.basename(FRONTERA_SPIDER_BUNDLE)
    run("tar --overwrite -xf %s" % fname)
    run("tar --overwrite -xf %s" % fc_name)

def generateSpiderConfigs():
    if env.host not in common.HOSTS["frontera_spiders"]:
        return

    tpl = open("config-templates/settings_tpl.py").read()
    rendered = tpl.format(kafka_location=common.KAFKA_HOSTS[0])
    open("settings.py", "w").write(rendered)
    put("settings.py", FRONTERA_SETTINGS_DIR)
    os.remove("settings.py")

    tpl = open("config-templates/webservice-scrapy-settings_tpl.py").read()
    rendered = tpl.format(zookeeper_location=common.ZK_HOSTS[0])
    open("scrapy-settings.py", "w").write(rendered)
    put("scrapy-settings.py", FRONTERA_SPIDER_DIR + "/topical-spiders/webservice_settings.py")
    os.remove("scrapy-settings.py")

    tpl = open("config-templates/spiderN_tpl.py").read()
    partitions = FRONTERA_CLUSTER_CONFIG['spider_partitions_map'][env.host]
    for instance_id in partitions:
        rendered = tpl.format(instance_id=instance_id)
        fname = "spider%d.py" % instance_id
        open(fname, "w").write(rendered)
        put(fname, FRONTERA_SETTINGS_DIR)
        os.remove(fname)

def generateWorkersConfigs():
    if env.host not in common.HOSTS["frontera_workers"]:
        return


    tpl = open("config-templates/workersettings_tpl.py").read()
    thrift_servers = str(", ").join(map(lambda rs_host: "'%s'" % rs_host, common.HBASE_RS))
    rendered = tpl.format(thrift_servers_list=thrift_servers,
                          partitions_count=FRONTERA_CLUSTER_CONFIG['spider_instances'],
                          kafka_location=common.KAFKA_HOSTS[0],
                          zookeeper_location=common.ZK_HOSTS[0])
    fh = open("workersettings.py", "w")
    fh.write(rendered)
    fh.close()
    put("workersettings.py", FRONTERA_SETTINGS_DIR)
    os.remove("workersettings.py")

    tpl = open("config-templates/strategyN_tpl.py").read()
    partitions = FRONTERA_CLUSTER_CONFIG['sw_partitions'][env.host]
    for sw_instance_id in partitions:
        rendered = tpl.format(sw_instance_id=sw_instance_id)
        fname = "strategy%s.py" % sw_instance_id
        open(fname, "w").write(rendered)
        put(fname, FRONTERA_SETTINGS_DIR)
        os.remove(fname)

def _create_and_put_startup_script(content, filename):
    fh = open(filename, "w")
    print >> fh, content
    fh.close()
    put(filename, "/etc/init", use_sudo=True)
    os.remove(filename)

def generateSpiderStartupScripts():
    spider_job = """
    instance $SPIDER_ID
manual
description "Topical crawler Scrapy instance"
setuid ubuntu
script
    cd %(spider_dir)s
    scrapy crawl score -s FRONTIER_SETTINGS=frontier.spider$SPIDER_ID --logfile=spider$SPIDER_ID.log -L INFO
end script
""" % {"spider_dir": FRONTERA_SPIDER_DIR}

    _create_and_put_startup_script(spider_job, "topical-spider.conf")

def generateWorkersStartupScripts():
    job_tpl = """
instance $WORKER_ID
manual
description "{descr}"
setuid ubuntu
script
    cd {spider_dir}
    {cmd}
end script
"""

    sw_job = job_tpl.format(
       spider_dir=FRONTERA_SPIDER_DIR,
       cmd="python -m fronteracrawler.worker.strategy --config frontier.strategy$WORKER_ID",
       descr="Frontera strategy worker slave for topical crawler"
    )
    _create_and_put_startup_script(sw_job, "frontera-strategy-worker.conf")

    sw_job_master = job_tpl.format(
       spider_dir=FRONTERA_SPIDER_DIR,
       cmd="python -m fronteracrawler.worker.strategy --config frontier.strategy$WORKER_ID --master",
       descr="Frontera strategy worker master for topical crawler"
    )

    _create_and_put_startup_script(sw_job_master, "frontera-strategy-worker-master.conf")

    fw_job = job_tpl.format(
        spider_dir=FRONTERA_SPIDER_DIR,
        cmd="python -m fronteracrawler.worker.main --config frontier.workersettings --no-batches --no-scoring",
        descr="Frontera common worker"
    )
    _create_and_put_startup_script(fw_job, "frontera-worker.conf")

    single_job_tpl = """
manual
description "{descr}"
setuid ubuntu
script
    cd {spider_dir}
    {cmd}
end script
"""
    b_job = single_job_tpl.format(
        spider_dir=FRONTERA_SPIDER_DIR,
        cmd="python -m fronteracrawler.worker.main --config frontier.workersettings --no-incoming --no-scoring",
        descr="Frontera new batches generator"
    )
    _create_and_put_startup_script(b_job, "frontera-batch-generator.conf")

    fs_job = single_job_tpl.format(
        spider_dir=FRONTERA_SPIDER_DIR,
        cmd="python -m fronteracrawler.worker.main --config frontier.workersettings --no-batches --no-incoming",
        descr="Frontera scoring worker"
    )
    _create_and_put_startup_script(fs_job, "frontera-scoring-worker.conf")

@runs_once
def prepareBundles():
    if os.path.exists(FRONTERA_SPIDER_DIRNAME):
        shutil.rmtree(FRONTERA_SPIDER_DIRNAME)
    if os.system("git clone -q %s %s" % (FRONTERA_SPIDER_REPO, FRONTERA_SPIDER_DIRNAME)):
        raise Exception("Git cloning error.")
    if os.system("tar --exclude=.git* -czf %s %s" % (FRONTERA_SPIDER_BUNDLE, FRONTERA_SPIDER_DIRNAME)):
        raise Exception("Taring error.")
    shutil.rmtree(FRONTERA_SPIDER_DIRNAME)

    if os.path.exists(FRONTERA_CRAWLER_DIRNAME):
        shutil.rmtree(FRONTERA_CRAWLER_DIRNAME)
    if os.system("git clone -q %s %s" % (FRONTERA_CRAWLER_REPO, FRONTERA_CRAWLER_DIRNAME)):
        raise Exception("Git cloning error.")
    if os.system("tar --exclude=.git* -czf %s %s" % (FRONTERA_CRAWLER_BUNDLE, FRONTERA_CRAWLER_DIRNAME)):
        raise Exception("Taring error.")
    shutil.rmtree(FRONTERA_CRAWLER_DIRNAME)

def bootstrapFrontera():
    if env.host not in common.HOSTS["frontera_spiders"] and env.host not in common.HOSTS["frontera_workers"]:
        return

    installDependencies(["build-essential", "libpython-dev", "python-dev", "python-pip", "python-twisted", "git",
                         "python-six", "libsnappy-dev"])
    cloneFrontera()
    prepareBundles()
    deploySpiders()

    sudo("pip install -q -r %s/requirements.txt" % FRONTERA_DEST_DIR)
    if env.host in common.HOSTS["frontera_spiders"]:
        installDependencies(["dnsmasq", "python-lxml", "python-openssl", "python-w3lib",
                             "python-cssselect"], pre_commands=False)
        setupDnsmasq()
        # manual nltk.download() is still needed there
        sudo("pip install -q nltk scrapy==0.24.6 kazoo")
        generateSpiderConfigs()
        generateSpiderStartupScripts()

    if env.host in common.HOSTS["frontera_workers"]:
        installDependencies(["python-lxml", "python-w3lib", "python-cssselect"], pre_commands=False)
        sudo("pip install -q nltk scrapy==0.24.6 kazoo")
        generateWorkersConfigs()
        generateWorkersStartupScripts()


def calcFronteraLayout():
    def cores_iter(hosts):
        for host in hosts:
            type = common.INSTANCES[host].instance_type
            info = common.EC2_INSTANCE_DATA[type]
            for i in range(info['cpucores']):
                yield (host, i)

    def get_cores_sum(hosts):
        return len(list(cores_iter(hosts)))

    def spider_partitions(hosts):
        partitionsMap = {}
        partition_id = 0
        for host, core_id in cores_iter(hosts):
            partitionsMap.setdefault(host, []).append(partition_id)
            partition_id += 1
        return partitionsMap

    def map_workers(it, instances_count):
        workerMap = {}
        for partition_id in range(instances_count):
            try:
                host, core_id = it.next()
            except StopIteration:
                raise Exception("Not enough cores for Frontera workers.")
            else:
                workerMap.setdefault(host, []).append(partition_id)
        return workerMap

    spider_cores_count = get_cores_sum(common.HOSTS['frontera_spiders'])
    workers_cores_count = get_cores_sum(common.HOSTS['frontera_workers'])
    FRONTERA_CLUSTER_CONFIG['spider_instances'] = spider_cores_count
    FRONTERA_CLUSTER_CONFIG['sw_instances'] = spider_cores_count / 4
    FRONTERA_CLUSTER_CONFIG['fw_instances'] = spider_cores_count / 4
    assert FRONTERA_CLUSTER_CONFIG['sw_instances'] + FRONTERA_CLUSTER_CONFIG['fw_instances'] <= workers_cores_count
    FRONTERA_CLUSTER_CONFIG['spider_partitions_map'] = spider_partitions(common.HOSTS['frontera_spiders'])

    it = cores_iter(common.HOSTS['frontera_workers'])
    FRONTERA_CLUSTER_CONFIG['sw_partitions'] = map_workers(it, FRONTERA_CLUSTER_CONFIG['sw_instances'])
    FRONTERA_CLUSTER_CONFIG['fw_partitions'] = map_workers(it, FRONTERA_CLUSTER_CONFIG['fw_instances'])


def _upstartCallSpiders(command):
    if env.host not in common.HOSTS["frontera_spiders"]:
        return

    partitions = FRONTERA_CLUSTER_CONFIG['spider_partitions_map'][env.host]
    with cd(FRONTERA_SPIDER_DIR):
        for instance_id in partitions:
            sudo("initctl %(cmd)s topical-spider SPIDER_ID=%(instance_id)d" % {"instance_id": instance_id,
                                                                               "cmd": command})

def _upstartCallWorkers(command):
    if env.host not in common.HOSTS["frontera_workers"]:
        return

    with cd(FRONTERA_SPIDER_DIR):
        partitions = FRONTERA_CLUSTER_CONFIG['sw_partitions'][env.host]
        sudo("initctl %(cmd)s frontera-strategy-worker-master WORKER_ID=%(instance_id)d" % {
            "instance_id": partitions[0],
            "cmd": command})

        for instance_id in partitions[1:]:
            sudo("initctl %(cmd)s frontera-strategy-worker WORKER_ID=%(instance_id)d" % {
                "instance_id": instance_id,
                "cmd": command})

        partitions = FRONTERA_CLUSTER_CONFIG['fw_partitions'][env.host]
        for instance_id in partitions:
            sudo("initctl %(cmd)s frontera-worker WORKER_ID=%(instance_id)d" % {
                "instance_id": instance_id,
                "cmd": command
            })

        sudo("initctl %s frontera-batch-generator" % command)
        sudo("initctl %s frontera-scoring-worker" % command)


def startSpiders():
    _upstartCallSpiders("start")

def stopSpiders():
    _upstartCallSpiders("stop")

def startWorkers():
    _upstartCallWorkers("start")

def stopWorkers():
    _upstartCallWorkers("stop")
