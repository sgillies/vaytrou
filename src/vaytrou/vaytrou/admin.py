from optparse import OptionParser
import os
import shutil
import sys

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree

from vaytrou.app import appmaker

class IndexAdmin(object):
    def __init__(self):
        self.opts = None
        self.args = None
        self.parser = OptionParser()
        self.parser.disable_interspersed_args()
        self.parser.add_option(
            "-d", "--data", dest="data",
            help="Data directory", metavar="FILE")
        self.opts = None
        self.args = None
    def run(self, args):
        self.opts, arguments = self.parser.parse_args(args)
        command, name = arguments[:2]
        getattr(self, command)(name, arguments[2:])
    def create(self, name, args):
        command_parser = OptionParser()
        command_parser.set_usage("create name [options]")
        #command_parser.add_option(
        #    "-x", "--xoo", dest="xoo",
        #    help="Xoo factor", metavar="FILE")
        # TODO: rtree and zodb creation parameters
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        os.mkdir(storage)
        finder = PersistentApplicationFinder(
            'file://%s/Data.fs' % storage, appmaker)
        environ = {}
        index = finder(environ)
        index.fwd = Rtree('%s/vrt1' % storage)
        index.commit()
        index.close()
        return 1
    def drop(self, name, args):
        command_parser = OptionParser()
        command_parser.set_usage("drop name [options]")
        #command_parser.add_option(
        #    "-x", "--xoo", dest="xoo",
        #    help="Xoo factor", metavar="FILE")
        # TODO: rtree and zodb deletion parameters
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        shutil.rmtree(storage)
        return 1

if __name__ == "__main__":
    admin = IndexAdmin()
    admin.run(sys.argv[1:])

