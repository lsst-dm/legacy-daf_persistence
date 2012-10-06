#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

# -*- python -*-

"""This module defines the ButlerFactory class."""

from lsst.daf.persistence import Butler

class ButlerFactory(object):
    """ButlerFactory creates data Butlers containing data mappers.  Use of it
    is deprecated in favor of the direct Butler constructor.
    
    The ButlerFactory class takes a mapper for a data collection.
    It can then create Butlers with these mappers.

    A data identifier is a dictionary.  The keys match those understood by a
    mapper; the values select particular data sets or collections of data
    sets.  For example, one key might be "visit".  Specifying a value of
    "695934" for this key might select a collection of images.

    The mappers perform four functions:
      1. Determine what keys are valid for dataset ids.
      2. Obtain a collection of potential dataset ids matching a
         partial dataset id.
      3. Map a dataset id to the location of the dataset, including its
         C++ and Python types.
      4. Manipulate a retrieved dataset object so that it conforms to a
         standard.

    Public methods:

    __init__(self, mapper)

    create(self)
    """

    def __init__(self, mapper):
        """Construct a ButlerFactory.

        @param mapper mapper object.
        """

        self.mapper = mapper

    def create(self):
        """Create a Butler.

        @returns a new Butler.
        """

        return Butler(None, mapper=self.mapper)
