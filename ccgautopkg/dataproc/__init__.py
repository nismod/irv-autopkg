

class Boundary(dict):
    """Encapsulates the definition of a boundary required for downstream processing"""

    def __init__(self, name, geojson):
        dict.__init__(self, name=name, geojson=geojson)
        self.name = name
        self.geojson = geojson