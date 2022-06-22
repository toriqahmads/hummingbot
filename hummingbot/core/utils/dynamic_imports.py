from importlib import import_module
from types import ModuleType


def import_module_with_plugin_support(module_name: str, module_package: str, parent_package: str) -> ModuleType:
    parent_module = import_module(f".{module_package}", package=parent_package)
    module = getattr(parent_module, module_name, None)
    if module is None:
        full_package = ".".join([parent_package, module_package])
        module = import_module(f".{module_name}", package=full_package)
    return module
