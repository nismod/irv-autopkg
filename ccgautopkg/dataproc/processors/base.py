

from abc import ABC

class BaseProcessorABC(ABC):
    """Base Processor ABC"""

    def generate(self):
        """Generate files for a given processor"""

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
