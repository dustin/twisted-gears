#!/usr/bin/env python
"""
TwistedGears installation script
"""
from setuptools import setup

setup(
    name = "twisted-gears",
    version = "0.2",
    author = "Dustin Sallings",
    author_email = "dustin@spy.net",
    url = "http://bleu.west.spy.net/~dustin/",
    description = "A twisted interface to gearman.",
    license="Unknown",
    packages = ["gearman"],
    classifiers = [
        "Framework :: Twisted",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Topic :: Communications",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ]
    )
