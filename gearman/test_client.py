from twisted.trial import unittest

from gearman.client import Buffer

class BufferTest(unittest.TestCase):

    def testBufferEmpty(self):
        b = Buffer()
        self.assertEquals(0, b.strLength())

    def testBufferWithData(self):
        b = Buffer()
        b.append("ab")
        b.append("blah")

        self.assertEquals("abblah", str(b))
        self.assertEquals(6, b.strLength())
        self.assertEquals("ab", b.popBytes(2))
        self.assertEquals(4, b.strLength())
        self.assertEquals("blah", str(b))

    def testBufferWithDataTwo(self):
        b = Buffer()
        b.append("ab")
        b.append("blah")

        self.assertEquals("abblah", str(b))
        self.assertEquals(6, b.strLength())
        self.assertEquals("abb", b.popBytes(3))
        self.assertEquals(3, b.strLength())
        self.assertEquals("lah", str(b))
