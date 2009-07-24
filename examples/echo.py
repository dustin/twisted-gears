#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

from gearman import client

@defer.inlineCallbacks
def worker(gearman):
    rv = yield gearman.echo()
    print "Echo 1 yielded %d: ``%s''" % rv
    rv = yield gearman.echo("something else")
    print "Echo 2 yielded %d: ``%s''" % rv

    d1 = gearman.echo("pipeline 1")
    d1.addCallback(lambda v: sys.stdout.write(
            "Echo 3 yielded %d: ``%s''\n" % v))
    d2 = gearman.echo("pipeline 2")
    d2.addCallback(lambda v: sys.stdout.write(
            "Echo 4 yielded %d: ``%s''\n" % v))

    dl = defer.DeferredList([d1, d2])
    yield dl

    print "OK.  We're done."

    reactor.stop()

d=protocol.ClientCreator(reactor, client.GearmanProtocol).connectTCP(
    sys.argv[1], 4730)
d.addCallback(worker)

reactor.run()

