#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#


def genericAssembler(dataId, componentInfo, cls):
    """A generic assembler for butler composite datasets, that can be used when the component names match the
    argument names in the __init__ signature, or the setter name for component objects is specified or can be
    inferred by component name.

    When determining setter names: If the setter name is specified by the policy then the genericAssembler
    will use that to set the component into the python object. If the policy does not specify setter names the
    genericAssembler will see if the __init__ func input argument names match the policy argument names. If
    that does not work, and the python object has setter names that match the component name of all the object
    then the setter name can be inferred; it will first try 'set' + <componentName>, and if that does not
    exist it will try 'set' + <componentName>.capitalize (e.g. for component name 'foo', it will try setfoo
    and then setFoo.) If no setter can be found for a component object, it will raise a runtime error.
    """
    initArgs = {k: v.obj for k, v in componentInfo.items()}
    try:
        obj = cls(**initArgs)
    except TypeError:
        obj = None

    if not obj:
        obj = cls()
        for componentName, componentInfo in componentInfo.items():
            if componentInfo.setter is not None:
                setter = getattr(obj, componentInfo.setter)
            elif hasattr(obj, 'set_' + componentName):
                setter = getattr(obj, 'set_' + componentName)
            elif hasattr(obj, 'set' + componentName.capitalize()):
                setter = getattr(obj, 'set' + componentName.capitalize())
            else:
                raise RuntimeError("No setter for datasetType:%s class:%s" %
                                   (componentInfo.datasetType, cls))
            setter(componentInfo.obj)
    return obj


def genericDisassembler(obj, dataId, componentInfo):
    """A generic disassembler for butler composite datasets, that can be used when the getter name for
    component objects is specified or can be inferred by component name.

    When determining getter names: If the getter name is specified by the policy then the genericAssembler
    will use that to get the component from the python object. If the policy does not specify getter names and
    the python object has getter names that match the component name of all the object then the getter name
    can be inferred; it will first try 'get' + <componentName>, and if that does not exist it will try 'get' +
    <componentName>.capitalize (e.g. for component name 'foo', it will try getfoo and then getFoo.) If no
    getter can be found for a component object, it will raise a runtime error.
    """
    for componentName, componentInfo in componentInfo.items():
        if componentInfo.getter is not None:
            getter = getattr(obj, componentInfo.getter)
        elif hasattr(obj, 'get_' + componentName):
            getter = getattr(obj, 'get_' + componentName)
        elif hasattr(obj, 'get' + componentName.capitalize()):
            getter = getattr(obj, 'get' + componentName.capitalize())
        else:
            raise RuntimeError("No getter for componentName:%s" % componentName)
        componentInfo.obj = getter()
