# -*- coding: utf-8 -*-
import os
from common import installDependencies
from fabric.api import run, cd, env, settings, put, sudo
from fabric.decorators import runs_once, parallel
from fabric.tasks import execute

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
    put("resolv.dnsmasq.conf")
    put("dnsmasq.conf")
    sudo("mv ~/resolv.dnsmasq.conf /etc/")
    sudo("mv ~/dnsmasq.conf /etc/")
    sudo("service dnsmasq restart")
    os.remove("dnsmasq.conf")
    os.remove("resolv.dnsmasq.conf")



def bootstrapSpiders():
    installDependencies(["dnsmasq", "g++", "libpython-dev", "python-dev", "python-lxml", "python-twisted",
                         "python-openssl", "python-w3lib", "python-cssselect", "python-six", "python-pip"])
    sudo("pip install -q happybase kafka-python nltk sqlalchemy tldextract queuelib")
    setupDnsmasq()

