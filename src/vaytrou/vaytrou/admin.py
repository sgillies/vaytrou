import inspect
from optparse import OptionParser
import os
import pprint
import shutil
import sys

from repoze.zodbconn.finder import PersistentApplicationFinder
from repoze.zodbconn.uri import db_from_uri
from rtree import Rtree
from rtree.index import Property
from simplejson import dumps, load
from ZODB import FileStorage, DB

from indexing import ChangeSet
from vaytrou.app import appmaker

class BaseCommand(object):
    def __init__(self):
        self.parser = OptionParser()
    def help(self):
        self.parser.print_help()
    def open(self, path, create=False, read_only=True):
        self.storage = FileStorage.FileStorage(
            "%s/Data.fs" % path, create=create, read_only=read_only)
        self.db = DB(self.storage)
        self.conn = self.db.open()
        root = self.conn.root()
        index = appmaker(root)
        index.fwd = Rtree("%s/vrt1" % path)
        return index
    def close(self):
        self.conn.close()
        self.db.close()
        self.storage.close()

class BatchCommand(BaseCommand):
    def __init__(self):
        super(BatchCommand, self).__init__()
        self.parser.set_usage("%prog [global options] batch name [options]")
        self.parser.description = "Submit a batch of items to be indexed or unindexed"
        self.parser.add_option(
            "-f", "--batch-file", dest="filename",
            help="Batch filename", metavar="FILE")
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        storage = os.path.join(container.opts.data, name)
        index = self.open(storage, read_only=False)
        data = load(open(copts.filename, 'rb'))
        changeset = ChangeSet(
            additions=data.get('index'), deletions=data.get('unindex'))
        index.batch(changeset)
        index.commit()
        index.close()
        self.close()

class CreateCommand(BaseCommand):
    def __init__(self):
        super(CreateCommand, self).__init__()
        self.parser.set_usage("%prog [global options] create name")
        self.parser.description = "Create and initialize storage for a new index"
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        path = os.path.join(container.opts.data, name)
        os.mkdir(path)
        index = self.open(path, create=True, read_only=False)
        index.commit()
        index.close()
        self.close()

class DumpCommand(BaseCommand):
    def __init__(self):
        super(DumpCommand, self).__init__()
        self.parser.set_usage("%prog [global options] dump name")
        self.parser.description = "Write all items, wrapped in an index batch, to stdout"
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        path = os.path.join(container.opts.data, name)
        index = self.open(path)
        count = len(index.bwd)
        print(dumps(dict(count=count, index=list(index.bwd.values()))))
        index.close()
        self.close()

class DropCommand(BaseCommand):
    def __init__(self):
        super(DropCommand, self).__init__()
        self.parser.set_usage("%prog [global options] drop name [options]")
        self.parser.description = "Drop index, deleting storage"
        self.parser.set_defaults(force=0)
        self.parser.add_option("-f", "--force", action="store_const",
            dest="force", const=1, help="Do not prompt for confirmation")
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        storage = os.path.join(container.opts.data, name)
        if not copts.force:
            sys.stdout.write("This command is irreversible. Continue? [y/N] ")
            char = sys.stdin.read(1)
            if char not in ("y", "Y"):
                print "Aborted."
                return None
        shutil.rmtree(storage)

class InfoCommand(BaseCommand):
    def __init__(self):
        super(InfoCommand, self).__init__()
        self.parser.set_usage("%prog [global options] info name")
        self.parser.description = "Write information about an index to stdout"
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        path = os.path.join(container.opts.data, name)
        index = self.open(path)
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
        index.close()
        self.close()

class SearchCommand(BaseCommand):
    def __init__(self):
        super(SearchCommand, self).__init__()
        self.parser.set_usage(
            "%prog [global options] search name [options] -- minx,miny,maxx,maxy")
        self.parser.description = "Execute a search on minx,miny,maxx,maxy bounding box and write results to stdout"
        self.parser.set_defaults(method="intersection")
        self.parser.add_option("--intersection", action="store_const",
            dest="method", const="intersection",
            help="return items that intersect with the bounding box (default)"
            )
        self.parser.add_option("--nearest", action="store_const",
                  dest="method", const="nearest",
                  help="return items nearest to the bounding box")
        self.parser.add_option("-l", "--limit", dest="limit", default="1",
                  help="limit number of results")
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        path = os.path.join(container.opts.data, name)
        index = self.open(path)
        coords = tuple(float(x) for x in cargs[0].split(','))
        if copts.method == "nearest":
            gen = index.nearest(coords, int(copts.limit))
        if copts.method == "intersection":
            gen = index.intersection(coords)
        results = list(gen)
        print(dumps(dict(count=len(results), items=results)))
        index.close()
        self.close()

class PackCommand(BaseCommand):
    def __init__(self):
        super(PackCommand, self).__init__()
        self.parser.set_usage("%prog [global options] pack name")
        self.parser.description = "Pack index's         storage, conserving disk space"
    def __call__(self, container, name, args):
        copts, cargs = self.parser.parse_args(args)
        data = os.path.join(container.opts.data, name)

        # First, pack the ZODB
        storage = FileStorage.FileStorage("%s/Data.fs" % data)
        db = DB(storage)
        db.pack()

        # Can't pack an Rtree's storage in-place, so we move it away and 
        # recreate from the contents of the ZODB
        rtree = None
        rtree_filename = '%s/vrt1' % data
 
        try:
            shutil.move(rtree_filename + ".dat", rtree_filename + ".bkup.dat")
            shutil.move(rtree_filename + ".idx", rtree_filename + ".bkup.idx")
        
            conn = db.open()
            root = conn.root()
            keys = root['index'].keys

            bkup = Rtree('%s/vrt1.bkup' % data)
            pagesize = bkup.properties.pagesize

            if len(keys) == 0:
                fwd = Rtree(
                        '%s/vrt1' % data,
                        # Passing in copied properties doesn't work,
                        # leading to errors involving page ids
                        # properties=new_properties, 
                        pagesize=pagesize
                        )
            else:
                gen = ((intid, bbox, None) for intid, (uid, bbox) \
                      in keys.items())
                fwd = Rtree(
                        '%s/vrt1' % data,
                        gen, 
                        # Passing in copied properties doesn't work,
                        # leading to errors involving page ids
                        # properties=new_properties,
                        pagesize=pagesize
                        )
            
            conn.close()
            db.close()
            storage.close()
        except:
            # Restore backups
            shutil.copy(rtree_filename + ".bkup.dat", rtree_filename + ".dat")
            shutil.copy(rtree_filename + ".bkup.idx", rtree_filename + ".idx")
            raise
        finally:
            if fwd is not None:
                fwd.close()

class IndexAdmin(object):
    def __init__(self):
        self.opts = None
        self.args = None
        self.parser = OptionParser(version="%prog 0.1")
        self.parser.disable_interspersed_args()
        self.parser.add_option(
            "-d", "--data", dest="data",
            help="Data directory", metavar="FILE")
        self.parser.set_usage(
        "%prog [program options] command [command options]\n\n"
        "Available commands are:\n\n"
        "batch: index or unindex a batch of items\n" 
        "create: create an index\n" 
        "drop: drop an index\n" 
        "dump: write contents of an index to stdout\n" 
        "help: print help\n" 
        "info: print information about an index\n" 
        "search: search an index for intersecting or nearest items\n" 
        "pack: pack index storage, saving disk space\n\n"
        "Help for a command can be had by:\n\n"
        "vtadmin help command\n\n"
        "or\n\n"
        "vtadmin command --help")
        self.opts = None
        self.args = None
        self.environ = {}
    def run(self, args):
        self.opts, arguments = self.parser.parse_args(args)
        command = arguments[0]
        # Command help?
        if arguments[1] in ("-h", "--help"):
            getattr(self, command).help()
        elif command == "help":
            getattr(self, arguments[1]).help()
        # Command execution
        else:
            name = arguments[1]
            getattr(self, command)(self, name, arguments[2:])

    batch = BatchCommand()
    create = CreateCommand()
    drop = DropCommand()
    dump = DumpCommand()
    info = InfoCommand()
    search = SearchCommand()
    pack = PackCommand()

