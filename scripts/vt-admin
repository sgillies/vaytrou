from functools import partial
import getopt
import math
import random
import sys

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree
from simplejson import load
from shapely.geometry import mapping, Point
from tornado.options import define, options
from tornado.options import parse_config_file, parse_command_line

from indexing import BatchError, ChangeSet, ConflictError
from vrtree import VIntRtreeIndex

def appmaker(root):
    if not 'index' in root:
        index = VIntRtreeIndex()
        root['index'] = index
        import transaction
        transaction.commit()
    return root['index']

index = None

define('data_directory', default=None, help='Data storage directory')

# TODO: Much of the code above and below could be shared with vaytrou.py
# server.

def batch(config_filename, batch_filename):
    parse_config_file(config_filename)
    finder = PersistentApplicationFinder(
        'file://%s/Data.fs' % options.data_directory, appmaker)
    environ = {}
    index = finder(environ)
    index.fwd = Rtree('%s/vrt1' % options.data_directory)
    data = load(open(batch_filename, 'rb'))
    changeset = ChangeSet(
        additions=data.get('index'), deletions=data.get('unindex'))
    index.batch(changeset)
    index.commit()

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:f:", ["config=", "batch-file="])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    config_filename = None
    batch_filename = None
    for o, a in opts:
        if o in ("-c", "--config"):
            config_filename = a
        elif o in ("-f", "--batch-file"):
            batch_filename = a
        else:
            assert False, "unhandled option"
    batch(config_filename, batch_filename)

if __name__ == "__main__":
    main()

