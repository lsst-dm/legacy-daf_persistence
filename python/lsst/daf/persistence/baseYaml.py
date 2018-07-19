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
    """Represent an lsst.daf.base.DateTime (as ISO8601-formatted string in TAI)
    """
    return dumper.represent_scalar(u'lsst.daf.base.DateTime',
                                   data.toString(lsst.daf.base.DateTime.TAI))


yaml.add_representer(lsst.daf.base.DateTime, dt_representer)


def pl_representer(dumper, data):
    """Represent an lsst.daf.base.PropertyList as an ordered sequence of
    name/type/value/comment tuples)"""
    result = lsst.daf.base.getPropertyListState(data)
    return dumper.represent_sequence(u'lsst.daf.base.PropertyList', result,
                                     flow_style=None)


yaml.add_representer(lsst.daf.base.PropertyList, pl_representer)


def ps_representer(dumper, data):
    """Represent an lsst.daf.base.PropertySet as a mapping from names to
    type/value pairs."""
    result = lsst.daf.base.getPropertySetState(data)
    return dumper.represent_sequence(u'lsst.daf.base.PropertySet', result,
                                     flow_style=None)


yaml.add_representer(lsst.daf.base.PropertySet, ps_representer)

###############################################################################

# YAML constructors for key lsst.daf.base classes


def dt_constructor(loader, node):
    """Construct an lsst.daf.base.DateTime from an ISO8601-formatted string in
    TAI"""
    dt = loader.construct_scalar(node)
    return lsst.daf.base.DateTime(str(dt), lsst.daf.base.DateTime.TAI)


yaml.add_constructor(u'lsst.daf.base.DateTime', dt_constructor)


def pl_constructor(loader, node):
    """Construct an lsst.daf.base.PropertyList from a pickle-state."""
    pl = lsst.daf.base.PropertyList()
    yield pl
    state = loader.construct_sequence(node, deep=True)
    lsst.daf.base.setPropertyListState(pl, state)


yaml.add_constructor(u'lsst.daf.base.PropertyList', pl_constructor)


def ps_constructor(loader, node):
    """Construct an lsst.daf.base.PropertyList from a pickle-state."""
    ps = lsst.daf.base.PropertySet()
    yield ps
    state = loader.construct_sequence(node, deep=True)
    lsst.daf.base.setPropertySetState(ps, state)


yaml.add_constructor(u'lsst.daf.base.PropertySet', ps_constructor)
