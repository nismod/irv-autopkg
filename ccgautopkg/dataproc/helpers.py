"""
Helper methods / classes
"""


class Boundary(dict):
    """Encapsulates the definition of a boundary"""

    def __init__(self, dbid, name, geojson, version):
        dict.__init__(self, dbid=dbid, name=name, geojson=geojson, version=version)
        self.dbid = dbid
        self.name = name
        self.geojson = geojson
        self.version = version
