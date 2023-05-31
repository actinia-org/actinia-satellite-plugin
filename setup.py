#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Setup file for actinia_satellite_plugin.

    This file was generated with PyScaffold 3.0.3.
    PyScaffold helps you to put up the scaffold of your new Python project.
    Learn more under: http://pyscaffold.org/
"""

import sys
from setuptools import setup

# Add here console scripts and other entry points in ini-style format
entry_points = """
[console_scripts]
# script_name = actinia_satellite_plugin.module:function
# For example:
# fibonacci = actinia_satellite_plugin.skeleton:run
"""


def setup_package():
    needs_sphinx = {"build_sphinx", "upload_docs"}.intersection(sys.argv)
    sphinx = ["sphinx"] if needs_sphinx else []
    setup(
        setup_requires=["pyscaffold>=3.3,<3.4"] + sphinx,
        entry_points=entry_points,
        use_pyscaffold=True,
    )


if __name__ == "__main__":
    setup_package()
