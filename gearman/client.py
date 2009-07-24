"""
Gearman client implementation.
"""

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
        self.deferreds = deque()

    def send_raw(self, cmd, data):
        """Send a command with the given data with no response."""

        self.transport.write(REQ_MAGIC)
        self.transport.write(struct.pack(">II", cmd, len(data)))
        self.transport.write(data)

    def send(self, cmd, data):
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
        # NOOPs don't *do* anything.
        if self.receivingCommand != NOOP:
            d = self.deferreds.popleft()
            d.callback((self.receivingCommand, data))
        self.receivingCommand = 0

        return self._headerReceived, HEADER_LEN

    def echo(self, data="hello"):
        """Send an echo request."""

        return self.send(ECHO_REQ, data)
