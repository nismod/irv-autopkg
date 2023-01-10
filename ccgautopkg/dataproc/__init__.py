

class Boundary(dict):
    """Encapsulates the definition of a boundary required for downstream processing"""

    def __init__(self, name, geojson, envelope_geojson):
        dict.__init__(self, name=name, geojson=geojson, envelope_geojson=envelope_geojson)
        self.name = name
        self.geojson = geojson
        self.envelope_geojson = envelope_geojson