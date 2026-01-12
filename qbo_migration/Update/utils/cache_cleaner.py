# utils/cache_cleaner.py

import sys
import os
import shutil
import gc

def clear_module_cache(modules_to_clear=None):
    """
    Clears selected or all cached modules from sys.modules.

    Args:
        modules_to_clear (list, optional): List of module name prefixes to clear (e.g., ['mapping.', 'storage.']).
                                            If None, no selective clearing will be done.
    """
    if modules_to_clear:
        for module in list(sys.modules):
            if any(module.startswith(prefix) for prefix in modules_to_clear):
                del sys.modules[module]
                print(f"🔁 Unloaded module: {module}")
    gc.collect()


def clear_pycache(start_path='.'):
    """
    Recursively removes __pycache__ folders.
    """
    for root, dirs, _ in os.walk(start_path):
        for dir_name in dirs:
            if dir_name == '__pycache__':
                full_path = os.path.join(root, dir_name)
                shutil.rmtree(full_path, ignore_errors=True)
                print(f"🧹 Removed: {full_path}")


def clear_global_lru_caches(functions: list = []):
    """
    Clears all LRU caches for passed-in functions.
    """
    for fn in functions:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
            print(f"🚿 Cleared LRU cache for: {fn.__name__}")


def clear_all_caches(module_prefixes=None, lru_functions=None, clear_files=True):
    """
    Full cache reset utility.
    - Clears LRU caches
    - Unloads cached modules
    - Removes __pycache__ dirs

    Args:
        module_prefixes (list): Module name prefixes to unload (optional)
        lru_functions (list): List of functions with lru_cache to clear
        clear_files (bool): Whether to remove __pycache__ folders
    """
    print("♻️  Clearing all cache...")
    clear_module_cache(module_prefixes)
    if lru_functions:
        clear_global_lru_caches(lru_functions)
    if clear_files:
        clear_pycache()
    gc.collect()
    print("✅ Cache cleanup complete.")



#Use case
# from utils.cache_cleaner import clear_all_caches
# from your_module import some_cached_function

# # Right before starting item migration
# clear_all_caches(
#     module_prefixes=["config.mapping", "storage.sqlserver"],  # e.g., mappings or sql module caches
#     lru_functions=[some_cached_function],  # any @lru_cache'd functions
#     clear_files=True  # removes __pycache__
# )

# # Now begin Item migration
# migrate_items()
