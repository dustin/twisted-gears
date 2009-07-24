"""
Gearman client implementation.
"""

import struct

from collections import deque

from twisted.internet import defer, protocol
from twisted.python import log

from constants import *

class Buffer(deque):
    """A read buffer that will treat chunks of data as a single string."""

    def strLength(self):
        """Get the length of the bytes in the string represented herein."""
        return sum([len(x) for x in self])

    def popBytes(self, n):
        """Remove n of the first bytes from the buffer."""
        stuff = []
        while sum([len(x) for x in stuff]) < n:
            stuff.append(self.popleft())

        stuff = ''.join(stuff)
        assert len(stuff) >= n

        # May need to shove some stuff back into the data buffer.
        if len(stuff) > n:
            self.appendleft(stuff[n:])
            stuff = stuff[:n]

        return stuff

    def __str__(self):
        rv = ''.join(self)
        return rv

class GearmanProtocol(protocol.Protocol):
    """Base protocol for handling gearman connections."""

    receivingCommand = 0
    toread = 0

    def __init__(self):
        self.current_data = Buffer()
        self.deferreds = deque()

    def _send_raw(self, cmd, data):
        """Send a command with the given data."""

        self.transport.write(REQ_MAGIC)
        self.transport.write(struct.pack(">II", cmd, len(data)))
        self.transport.write(data)

    def _send(self, cmd, data):
        self._send_raw(cmd, data)
        d = defer.Deferred()
        self.deferreds.append(d)
        return d

    def connectionLost(self, reason):
        try:
            for d in self.deferreds:
                d.errback(reason)
        except:
            log.err()
        self.deferreds.clear()
        self.current_data.clear()

    def _headerReceived(self, cmd, size):
        self.receivingCommand = cmd
        self.toread = size

    def _completed(self, data):
        d = self.deferreds.popleft()
        d.callback(data)
        self.receivingCommand = 0
        self.toread = 0

    def dataReceived(self, data):
        self.current_data.append(data)
        more = True

        while more:
            if self.receivingCommand:
                if self.current_data.strLength() >= self.toread:
                    data = self.current_data.popBytes(self.toread)
                    self._completed(data)
            else:
                if self.current_data.strLength() >= HEADER_LEN:
                    header = self.current_data.popBytes(HEADER_LEN)
                    if header[:4] != RES_MAGIC:
                        log.msg("Invalid header magic returned, failing.")
                        self.transport.loseConnection()
                        return
                    cmd, size = struct.unpack(">II", header[4:])
                    self._headerReceived(cmd, size)
                else:
                    more = False

    def echo(self, data="hello"):
        """Send an echo request."""

        return self._send(ECHO_REQ, data)
