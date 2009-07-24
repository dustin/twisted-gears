"""
Gearman client implementation.
"""

import sys
import struct

from collections import deque

from twisted.internet import defer
from twisted.protocols import stateful
from twisted.python import log

from constants import *

__all__ = ['GearmanProtocol']

class GearmanProtocol(stateful.StatefulProtocol):
    """Base protocol for handling gearman connections."""

    def makeConnection(self, transport):
        stateful.StatefulProtocol.makeConnection(self, transport)
        self.receivingCommand = 0
        self.sleep_deferred = None
        self.deferreds = deque()

    def send_raw(self, cmd, data=''):
        """Send a command with the given data with no response."""

        self.transport.write(REQ_MAGIC)
        self.transport.write(struct.pack(">II", cmd, len(data)))
        self.transport.write(data)

    def send(self, cmd, data=''):
        """Send a command and get a deferred waiting for the response."""
        self.send_raw(cmd, data)
        d = defer.Deferred()
        self.deferreds.append(d)
        return d

    def getInitialState(self):
        return self._headerReceived, HEADER_LEN

    def connectionLost(self, reason):
        try:
            for d in self.deferreds:
                d.errback(reason)
        except:
            log.err()
        self.deferreds.clear()

    def _headerReceived(self, header):
        if header[:4] != RES_MAGIC:
            log.msg("Invalid header magic returned, failing.")
            self.transport.loseConnection()
            return
        cmd, size = struct.unpack(">II", header[4:])

        self.receivingCommand = cmd
        return self._completed, size

    def _completed(self, data):
        # NOOPs wake listeners
        if self.receivingCommand == NOOP:
            assert self.sleep_deferred
            self.sleep_deferred.callback(None)
            self.sleep_deferred = None
        else:
            assert not self.sleep_deferred
            d = self.deferreds.popleft()
            d.callback((self.receivingCommand, data))
        self.receivingCommand = 0

        return self._headerReceived, HEADER_LEN

    def pre_sleep(self):
        """Enter a sleep state."""
        if not self.sleep_deferred:
            self.sleep_deferred = defer.Deferred()
            self.send_raw(PRE_SLEEP)
        return self.sleep_deferred

    def echo(self, data="hello"):
        """Send an echo request."""

        return self.send(ECHO_REQ, data)

class GearmanJob(object):
    """A gearman job."""

    def __init__(self, raw_data):
        self.handle, self.function, self.data = raw_data.split("\0", 2)

    def __repr__(self):
        return "<GearmanJob %s func=%s with %d bytes of data>" % (self.handle,
                                                                  self.function,
                                                                  len(self.data))

class GearmanWorker(object):
    """A gearman worker."""

    def __init__(self, protocol):
        self.protocol = protocol
        self.functions = {}

    def registerFunction(self, name, func):
        """Register the ability to perform a function."""

        self.functions[name] = func
        self.protocol.send_raw(CAN_DO, name)

    def getJob(self):

        def _grab_job_res(stuff):
            if stuff[0] == JOB_ASSIGN:
                return GearmanJob(stuff[1])
            else:
                return self.protocol.pre_sleep().addCallback(self.getJob)

        d = self.protocol.send(GRAB_JOB)
        d.addCallback(_grab_job_res)
        d.addErrback(lambda x: sys.stderr.write("Got err %s\n" % x))
        return d

    def _finishJob(self, job):
        def _respond(x):
            if x is None:
                x = ""
            self.protocol.send_raw(WORK_COMPLETE, job.handle + "\0" + x)

        def _fail(x):
            self.protocol.send_raw(WORK_EXCEPTION, job.handle + "\0" + str(x))

        f = self.functions[job.function]
        d = defer.maybeDeferred(f, job.data)
        d.addCallback(_respond)
        d.addErrback(_fail)

    def __iter__(self):
        while True:
            yield self.getJob().addCallback(self._finishJob)
