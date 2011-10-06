#!/usr/bin/env python

import subprocess
import readline
import rlcompleter
import sys
import time

APPLICATION = "thumbingbird"
BASTION="nest2.corp.twitter.com"
LAYOUT_SIZE = "10G"

APP_APP_PKGS = [
    ("centos-5.5-x86_64", "latest"),
    ("daemon-0.6.3-x86_64", "latest"),
    ("jdk-6u24-x86_64", "latest"),
    ("imagemagick-6.6.1-x86_64", "latest"),
    ("curl-7.15.5-9-x86_64", "latest"),
    ("vim-minimal-7.0.109-6-x86_64", "latest"),
    ("libidn-0.6.5-1-x86_64", "latest"),
    ("%s_config" % APPLICATION, "latest"),
    ("%s_secure" % APPLICATION, "latest"),
    ("%s_jni-x86_64" % APPLICATION, "latest"),
    (APPLICATION, "latest"),
    ]

class Completer:
    def __init__(self, words):
        self.words = words
        self.prefix = None
    def complete(self, prefix, index):
        if prefix != self.prefix:
            # we have a new prefix!
            # find all words that start with this prefix
            self.matching_words = [
                w for w in self.words if w.startswith(prefix)
                ]
            self.prefix = prefix
        try:
            return self.matching_words[index]
        except IndexError:
            return None

def usage():
    print "Usage: deployer {smfd|smf1} {layout|deploy}"
    exit(1)

def lookupHosts(datacenter, role):
    proc = subprocess.Popen(["ssh", BASTION,
                             "loony --dc=%s -l \"%%facts:hostname%%\" -g 'role:%s'" % (datacenter, role)],
                            stdout=subprocess.PIPE)
    return [line.strip() for line in proc.stdout.readlines()]

def layout_create(dc, role):
    dc_pkg = [("%s_%s" % (APPLICATION, dc), "latest")]
    pkgs = " ".join(["-p '%s=%s'" % pkg for pkg in (APP_APP_PKGS + dc_pkg)])

    cmd_args = {
        'app': APPLICATION,
        'size': LAYOUT_SIZE,
        'pkgs': pkgs,
        }
    cmd_tmpl = 'app layout_create -n %(app)s -s %(size)s %(pkgs)s'
    loony_cmd_args = {
        'dc': dc,
        'role': role,
        'cmd': cmd_tmpl % cmd_args
        }
    loony_cmd_tmpl = 'loony --dc=%(dc)s -g role:%(role)s -S run %(cmd)r'
    loony_cmd = loony_cmd_tmpl % loony_cmd_args

    subprocess.Popen(['ssh', '-t', BASTION, loony_cmd]).communicate()

def layout_list(host):
    proc = subprocess.Popen(["ssh", host,
                             "app layout_list -n %s -F revision -q" % (APPLICATION,)],
                            stdout=subprocess.PIPE)
    return [line.strip() for line in proc.stdout.readlines()]


def deploy(dc, role):
    hosts = lookupHosts(dc, role)
    revisions = layout_list(hosts[0])
    default_rev = revisions[-1]
    print "\n".join(["%s" % rev for rev in revisions])

    readline.set_completer_delims('\n')
    completer = Completer(revisions)
    readline.set_completer(completer.complete)
    readline.parse_and_bind("tab: complete")
    while True:
        revision = raw_input("Revision [%s]: " % default_rev)
        if revision in revisions:
            break
        elif revision == "":
            revision = default_rev
            break

    cmd_args = {
        'app': APPLICATION,
        'rev': revision
        }
    cmd_tmpl = '; '.join(
        ['monit stop %(app)s',
         'sleep 16',
         'app layout_monit -n %(app)s -r %(rev)s -a',
         'monit start %(app)s',
         ])
    loony_cmd_args = {
        'dc': dc,
        'role': role,
        'cmd': cmd_tmpl % cmd_args
        }
    loony_cmd_tmpl = 'loony --dc=%(dc)s -g role:%(role)s -S run %(cmd)r'
    loony_cmd = loony_cmd_tmpl % loony_cmd_args
    subprocess.Popen(['ssh', '-t', BASTION, loony_cmd]).communicate()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()

    if sys.argv[1] in ['smfd', 'smf1']:
        dc = sys.argv[1]
    else:
        usage()

    if sys.argv[2] == 'layout':
        action = layout_create
    elif sys.argv[2] == 'deploy':
        action = deploy
    else:
        usage()

    if dc == 'smfd':
        role = 'thumbingbird.devel'
    else:
        role = 'thumbingbird'

    action(dc, role)
