# This file is part of daf_persistence
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org/).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""This module defines YAML I/O for key lsst.daf.base classes."""

import yaml

import lsst.daf.base

# YAML representers for key lsst.daf.base classes


def dt_representer(dumper, data):
    """Represent an lsst.daf.base.DateTime (as ISO8601-formatted string in UTC)
    """
    return dumper.represent_scalar(u'lsst.daf.base.DateTime',
                                   data.toString(lsst.daf.base.DateTime.UTC))


yaml.add_representer(lsst.daf.base.DateTime, dt_representer)


def pl_representer(dumper, data):
    """Represent an lsst.daf.base.PropertyList as an ordered sequence of
    name/value/comment triples)"""
    pairList = []
    for name in data.getOrderedNames():
        pairList.append([name, data.get(name), data.getComment(name)])
    return dumper.represent_sequence(u'lsst.daf.base.PropertyList', pairList,
                                     flow_style=None)


yaml.add_representer(lsst.daf.base.PropertyList, pl_representer)


def ps_representer(dumper, data):
    """Represent an lsst.daf.base.PropertySet as a mapping from names to
    values."""
    result = {}
    for name in data.names(True):
        result[name] = data.get(name)
    return dumper.represent_mapping(u'lsst.daf.base.PropertySet', result,
                                    flow_style=None)


yaml.add_representer(lsst.daf.base.PropertySet, ps_representer)

###############################################################################

# YAML constructors for key lsst.daf.base classes


def dt_constructor(loader, node):
    """Construct an lsst.daf.base.DateTime from an ISO8601-formatted string in
    UTC"""
    dt = loader.construct_scalar(node)
    return lsst.daf.base.DateTime(str(dt), lsst.daf.base.DateTime.UTC)


yaml.add_constructor(u'lsst.daf.base.DateTime', dt_constructor)


def pl_constructor(loader, node):
    """Construct an lsst.daf.base.PropertyList from a sequence of
    name/value/comment triples."""
    pl = lsst.daf.base.PropertyList()
    yield pl
    pairList = loader.construct_sequence(node, deep=True)
    for (name, value, comment) in pairList:
        pl.set(name, value, comment)


yaml.add_constructor(u'lsst.daf.base.PropertyList', pl_constructor)


def ps_constructor(loader, node):
    """Construct an lsst.daf.base.PropertyList from a mapping from names to
    values."""
    ps = lsst.daf.base.PropertySet()
    yield ps
    d = loader.construct_mapping(node, deep=True)
    for name, value in d.items():
        ps.set(name, value)


yaml.add_constructor(u'lsst.daf.base.PropertySet', ps_constructor)
