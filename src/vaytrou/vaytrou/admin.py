from optparse import OptionParser
import os
import pprint
import shutil
import sys

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree
from simplejson import dumps, load

from indexing import ChangeSet
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
        self.environ = {}
    def find_index(self, storage):
        finder = PersistentApplicationFinder(
            'file://%s/Data.fs' % storage, appmaker)
        index = finder(self.environ)
        index.fwd = Rtree('%s/vrt1' % storage)
        return index
    def run(self, args):
        self.opts, arguments = self.parser.parse_args(args)
        command, name = arguments[:2]
        getattr(self, command)(name, arguments[2:])
    def batch(self, name, args):
        command_parser = OptionParser()
        command_parser.set_usage("create name [options]")
        command_parser.add_option(
            "-f", "--batch-file", dest="filename",
            help="Batch filename", metavar="FILE")
        # TODO: rtree and zodb creation parameters
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        index = self.find_index(storage)
        data = load(open(copts.filename, 'rb'))
        changeset = ChangeSet(
            additions=data.get('index'), deletions=data.get('unindex'))
        index.batch(changeset)
        index.commit()
        index.close()
    def create(self, name, args):
        command_parser = OptionParser()
        command_parser.set_usage("create name [options]")
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        os.mkdir(storage)
        index = self.find_index(storage)
        index.commit()
        index.close()
    def drop(self, name, args):
        command_parser = OptionParser()
        command_parser.set_usage("drop name [options]")
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        shutil.rmtree(storage)
    def dump(self, name, args):
        """Write all contents of the index to stdout

        Contents are wrapped in an "index" object, so the output can be
        submitted to the batch command.
        """
        command_parser = OptionParser()
        command_parser.set_usage("dump name [options]")
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        index = self.find_index(storage)
        count = len(index.bwd)
        print(dumps(dict(count=count, index=list(index.bwd.values()))))
    def info(self, name, args):
        """Write index information to stdout"""
        command_parser = OptionParser()
        command_parser.set_usage("info name [options]")
        copts, cargs = command_parser.parse_args(args)
        storage = os.path.join(self.opts.data, name)
        index = self.find_index(storage)
        # TODO: What do we want to get from the index? Tree params?
        count = len(index.bwd)
        bounds = tuple(index.fwd.bounds)
        print("Count:")
        print count
        print("Bounds:")
        pprint.pprint(bounds)
        #print("Tree:")
        #pprint.pprint(index.fwd.properties)
        #pprint.pprint(dir(index.fwd.properties))
    def search(self, name, args):
        """Execute intersection and nearest neighbor searches"""
        command_parser = OptionParser()
        command_parser.set_usage("search name [options] minx,miny,maxx,maxy")
        command_parser.set_defaults(method="intersection")
        command_parser.add_option("--intersection", action="store_const",
                  dest="method", const="intersection")
        command_parser.add_option("--nearest", action="store_const",
                  dest="method", const="nearest")
        command_parser.add_option("-l", "--limit", dest="limit", default="1")
        copts, cargs = command_parser.parse_args(args)
        assert len(cargs) == 1
        storage = os.path.join(self.opts.data, name)
        index = self.find_index(storage)
        coords = tuple(float(x) for x in cargs[0].split(','))
        if copts.method == "nearest":
            gen = index.nearest(coords, int(copts.limit))
        if copts.method == "intersection":
            gen = index.intersection(coords)
        results = list(gen)
        print(dumps(dict(count=len(results), items=results)))
    def pack(self, name, args):
        """Pack index storage"""
        raise NotImplementedError

if __name__ == "__main__":
    admin = IndexAdmin()
    admin.run(sys.argv[1:])

