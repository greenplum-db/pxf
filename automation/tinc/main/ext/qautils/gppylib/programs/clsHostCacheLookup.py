#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved.
#
#
# Used to inject faults into the file replication code
#

#
# THIS IMPORT MUST COME FIRST
#
# import mainUtils FIRST to get python version check
from qautils.gppylib.mainUtils import *

from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE
from qautils.gppylib.gpparseopts import OptParser,OptChecker
from qautils.gppylib.gphostcache import *

logger = gplog.get_default_logger()

#-------------------------------------------------------------------------
class GpHostCacheLookup:
    #
    # Constructor:
    #
    # @param options the options as returned by the options parser
    #
    def __init__(self, options):
        self.__options = options
        self.__pool = None

    ######
    def run(self):
        self.__pool = base.WorkerPool(1)

        interfaces = []
        hostNames = []
        for line in sys.stdin:
            interfaces.append(line.strip())
            hostNames.append(None)

        lookup = GpInterfaceToHostNameCache(self.__pool, interfaces, hostNames)

        for interface in interfaces:
            hostname = lookup.getHostName(interface)
            if hostname is None:
                sys.stdout.write("__lookup_of_hostname_failed__\n")
            else:
                sys.stdout.write(hostname)
                sys.stdout.write("\n")

        return 0 # success -- exit code 0!

    def cleanup(self):
        if self.__pool:
            self.__pool.haltWork()
            self.__pool.joinWorkers()
            self.__pool.join()

    #-------------------------------------------------------------------------
    @staticmethod
    def createParser():
        description = ("""
        This utility is NOT SUPPORTED and is for internal-use only.

        Used to look up hostnames from interface names
        """)

        help = [""""""]

        parser = OptParser(option_class=OptChecker,
                    description='  '.join(description.split()),
                    version='%prog version main build 25763')

        addStandardLoggingAndHelpOptions(parser, False)

        parser.setHelp(help)

        parser.set_defaults()
        return parser

    @staticmethod
    def createProgram(options, args):
        if len(args) > 0 :
            raise ProgramArgumentValidationException(\
                            "too many arguments: only options may be specified")
            
        return GpHostCacheLookup(options)
