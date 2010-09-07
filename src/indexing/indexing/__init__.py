#

from geojson import Feature

from indexing.bounds import calc

class Item(Feature):
    @property
    def bbox(self):
        return calc[self.geometry.type](self.geometry)


