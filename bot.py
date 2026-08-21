#!/usr/bin/env python3
"""Compatibility entry point.

v20 moved to a modular package (app/).  This shim keeps the legacy start
command `python bot.py` working by delegating to the new entry point.
"""
from run import main

if __name__ == "__main__":
    raise SystemExit(main())
