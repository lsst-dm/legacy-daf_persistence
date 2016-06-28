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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import copy
import UserDict

class DataId(UserDict.IterableUserDict):
    """DataId is used to pass scientifically meaninful key-value pairs. It may be tagged as applicable only
    repositories that are tagged with the same value"""

    def __init__(self, initialdata=None, tag=None, **kwargs):
        """Constructor

        Parameters
        -----------
        initialdata : dict
            A dict of inital data for the DataId
        tag : any type, or a container of any type
            A value or container of values used to restrict the DataId to one or more repositories that 
            share that tag value. It will be stored in a set for comparison with the set of tags assigned to 
            repositories.
        kwargs : any values
            key-value pairs to be used as part of the DataId's data.
        """
        UserDict.UserDict.__init__(self, initialdata)
        try:
            self.tag = copy.deepcopy(initialdata.tag)
        except AttributeError:
            self.tag = set()

        if tag is not None:
            if isinstance(tag, basestring):
                self.tag.update([tag])
            else:
                try:
                    self.tag.update(tag)
                except TypeError:
                    self.tag.update([tag])
            
        self.data.update(kwargs)

    def __repr__(self):
        return "DataId(initialdata=%s, tag=%s)" %(self.data.__repr__(), self.tag)
