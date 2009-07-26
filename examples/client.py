#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task
from twisted.python import util

from gearman import client

@defer.inlineCallbacks
def gclient(gearman):
    w = client.GearmanClient(gearman)

    x = yield w.submit('test', 'some data')
    print "result:", repr(x)
    reactor.stop()

d=protocol.ClientCreator(reactor, client.GearmanProtocol).connectTCP(
    sys.argv[1], 4730)
d.addCallback(gclient)

# sys.settrace(util.spewer)

reactor.run()

