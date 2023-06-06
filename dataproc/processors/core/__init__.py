import pkgutil
import warnings

__all__ = []
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    __all__.append(module_name)
    try:
        _module = loader.find_module(module_name).load_module(module_name)
        globals()[module_name] = _module
    except Exception as err:
        warnings.warn(f"failed to load module {module_name} due to {err}")
