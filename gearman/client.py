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

__all__ = ['GearmanProtocol', 'GearmanWorker']

class GearmanProtocol(stateful.StatefulProtocol):
    """Base protocol for handling gearman connections."""

    def makeConnection(self, transport):
        stateful.StatefulProtocol.makeConnection(self, transport)
        self.receivingCommand = 0
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
            for d in list(self.deferreds):
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
        d = self.deferreds.popleft()
        d.callback((self.receivingCommand, data))
        self.receivingCommand = 0

        return self._headerReceived, HEADER_LEN

    def pre_sleep(self):
        """Enter a sleep state."""
        return self.send(PRE_SLEEP)

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

    def _send_job_res(self, cmd, job, data=''):
        self.protocol.send_raw(cmd, job.handle + "\0" + data)

    @defer.inlineCallbacks
    def getJob(self):
        """Get the next job."""
        stuff = yield self.protocol.send(GRAB_JOB)
        while stuff[0] == NO_JOB:
            yield self.protocol.pre_sleep()
            stuff = yield self.protocol.send(GRAB_JOB)
        defer.returnValue(GearmanJob(stuff[1]))

    @defer.inlineCallbacks
    def _finishJob(self, job):
        assert job
        f = self.functions[job.function]
        assert f
        try:
            rv = yield f(job.data)
            if rv is None:
                rv = ""
            self._send_job_res(WORK_COMPLETE, job, rv)
        except:
            x = sys.exc_info()
            self._send_job_res(WORK_EXCEPTION, job, str(x))
            self._send_job_res(WORK_FAIL, job)

    def __iter__(self):
        while True:
            yield self.getJob().addCallback(self._finishJob)
