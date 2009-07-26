#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task
from twisted.python import util

from gearman import client

def run_test(j):
    print repr(j)
    return j

# @defer.inlineCallbacks
def worker(gearman):
    w = client.GearmanWorker(gearman)
    w.setId("exampleworker")
    w.registerFunction("test", run_test)

    coop = task.Cooperator()
    for i in range(5):
        ## Ramp up start
        reactor.callLater(0.1 * i, lambda: coop.coiterate(w.doJobs()))
        ## Immediate start
        # coop.coiterate(iter(w))

d=protocol.ClientCreator(reactor, client.GearmanProtocol).connectTCP(
    sys.argv[1], 4730)
d.addCallback(worker)

# sys.settrace(util.spewer)

reactor.run()

