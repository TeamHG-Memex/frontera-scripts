# -*- coding: utf-8 -*-
import os
from common import installDependencies
import common
import json
from fabric.api import run, cd, env, settings, put, sudo
from fabric.decorators import runs_once, parallel
from fabric.tasks import execute

EC2_INSTANCE_DATA = {}
FRONTERA_TAG = "v.0"
FRONTERA_DEST_DIR = "/home/ubuntu/frontera"
FRONTERA_CLUSTER_CONFIG = {
    "spider_instances": 0,
    "sw_instances": 0,
    "fw_instances": 0
}

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
        run("git checkout -q %s" % FRONTERA_TAG)

    # adding Frontera to python module path
    python_path = "/usr/lib/python2.7/dist-packages/ubuntu.pth"
    sudo("rm -f %s" % python_path)
    fh = open("ubuntu.pth", "w")
    print >> fh, FRONTERA_DEST_DIR
    fh.close()
    put("ubuntu.pth", python_path, use_sudo=True)
    os.remove("ubuntu.pth")


def bootstrapSpiders():
    if env.host not in common.HOSTS["frontera_spiders"] and env.host not in common.HOSTS["frontera_workers"]:
        return
    installDependencies(["dnsmasq", "build-essential", "libpython-dev", "python-dev", "python-lxml", "python-twisted",
                         "python-openssl", "python-w3lib", "python-cssselect", "python-six", "python-pip", "git"])
    cloneFrontera()

    sudo("pip install -q nltk scrapy")
    sudo("pip install -q -r %s/requirements.txt" % FRONTERA_DEST_DIR)
    setupDnsmasq()

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

def configureFrontera():
    def get_cores_sum(hosts):
        _sum = 0
        for host in hosts:
            type = common.INSTANCES[host].instance_type
            info = EC2_INSTANCE_DATA[type]
            _sum += info['cpucores']
        return _sum

    _load_ec2_data()
    spider_cores_count = get_cores_sum(common.HOSTS['frontera_spiders'])
    workers_cores_count = get_cores_sum(common.HOSTS['frontera_workers'])
    FRONTERA_CLUSTER_CONFIG['spider_instances'] = spider_cores_count
    FRONTERA_CLUSTER_CONFIG['sw_instances'] = spider_cores_count / 4
    FRONTERA_CLUSTER_CONFIG['fw_instances'] = spider_cores_count / 4
    assert FRONTERA_CLUSTER_CONFIG['sw_instances'] + FRONTERA_CLUSTER_CONFIG['fw_instances'] <= workers_cores_count

