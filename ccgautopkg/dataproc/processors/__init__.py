

# from .core.raster_processor_one import RasterProcessorOne
# from .core.raster_processor_two import RasterProcessorTwo
# from .core.raster_processor_three import RasterProcessorThree


# # Working but potentially unneeded
# import importlib
# import os
# from inspect import isclass
# from pathlib import Path
# from pkgutil import iter_modules
# # Selectively externalise Processor classes
# # This allows us to import * from the Processors module, then use the classes directly in later pipeline
# package_dir = os.path.dirname(os.path.abspath(__file__))
# for (_, module_name, _) in iter_modules([package_dir]):
#     if 'processor' in module_name:
#         module = importlib.import_module(f".{module_name}", package='processors')
#         for attribute_name in dir(module):
#             if 'Processor' in attribute_name:
#                 attribute = getattr(module, attribute_name)
#                 if isclass(attribute) and issubclass(attribute, BaseABC):
#                     globals()[attribute_name] = attribute
