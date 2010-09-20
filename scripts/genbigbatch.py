from functools import partial
import getopt
import math
import random
import sys

from simplejson import dumps
from shapely.geometry import mapping, Point

def batch(num_features):
    # Coordinate values in range [0, 50]
    x = partial(random.uniform, 0.0, 50.0)
    # radii in range [0.01, 0.5]
    r = partial(random.uniform, 0.01, 0.5)
    # Poisson-distributed resolution k
    def k(expectation=1):
        #partial(random.randint, 1, 4)
        L = math.exp(-expectation)
        k = 0
        p = 1
        while p > L:
            k = k + 1
            u = random.uniform(0.0, 1.0)
            p = p * u
        return k - 1


    batch = {'index': []}
    for i in xrange(num_features):
        point = Point(x(), x())
        polygon = point.buffer(r(), k())
        batch['index'].append(dict(
            id=str(i+1), 
            bbox=polygon.bounds, 
            geometry=mapping(polygon), 
            properties=dict(title='Feature %d' % (i+1)))
            )
    return batch

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "n:", ["number-features="])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    num_features = None
    for o, a in opts:
        if o in ("-n", "--number-features"):
            num_features = int(a)
        else:
            assert False, "unhandled option"
    
    print(dumps(batch(num_features)))

if __name__ == "__main__":
    main()

