"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.daf.persistence


_g = globals()
_g.update(build_package_configs(
    project_name='daf_persistence',
    version=lsst.daf.persistence.version.__version__))
