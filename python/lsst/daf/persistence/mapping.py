#!/bin/env python
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os
import re
from lsst.daf.persistence import ButlerLocation
import lsst.pex.policy as pexPolicy

"""This module defines the Mapping base class."""

class Mapping(object):

	"""Mapping is a base class for all mappings.  Mappings are used by
	the Mapper to map (determine a path to some data given some
	identifiers) and standardize (convert data into some standard
	format or type) data, and to query the associated registry to see
	what data is available.

	Public methods: map, canStandardize, standardize, lookup, have, need

	Mappings are specified mainly by policy.  A Mapping policy should
	consist of:

	template (string): a Python string providing the filename for that
	particular data set type based on some data identifiers.  In the
	case of redundancy in the path (e.g., file uniquely specified by
	the exposure number, but filter in the path), the
	redundant/dependent identifiers can be looked up in the registry.

	python (string): the Python type for the data (e.g.,
	lsst.afw.image.ExposureF)

	cpp (string): the C++ type for the data (e.g., ExposureF)

	storage (string): the storage type for the data (e.g., FitsStorage)

	level (string): the level in the camera hierarchy at which the
	data is stored (Amp, Ccd or skyTile)

	In addition, the following optional entries are permitted:

	map (string): the name of a function in the appropriate Mapper
	subclass that will map a data identifier to a path.  The function
	will receive: the Mapper, this Mapping, and the data identifier
	dict.

	standardize (string): the name of a function in the appropriate
	Mapper subclass that will standardize the input data.  The
	function will receive: the Mapper, this Mapping, the item to
	standardize, and the data identifier dict.  The special value
	"None" means no standardization is performed.

	tables (string): a whitespace-delimited list of tables in the
	registry that can be NATURAL JOIN-ed to look up additional
	information.

	query (string): a Python string providing a SQL query to look up
	the registry.  It should typically start as "SELECT *".

	lookup (string): the name of a function in the appropriate Mapper
	subclass that will lookup the desired properties.  The function
	will receive: the Mapper, this Mapping, a list of the desired
	properties, and the data identifier dict.	
	"""

	def __init__(self,					# Mapping object
				 mapper=None,			# Mapper object
				 policy=None,			# Policy for mapping
				 type=None,				# Type of mapping
				 registry=None,			# Registry for lookups
				 root=None				# Root directory
				 ):
		if mapper is None:
			raise RuntimeError, "No mapper provided for mapping"
		if policy is None:
			raise RuntimeError, "No policy provided for mapping"

		self.type = type
		self.registry = registry
		self.root = root

		self.template = policy.getString("template") # Template path
		self.python = policy.getString("python") # Python type
		self.cpp = policy.getString("cpp") # C++ type
		self.storage = policy.getString("storage") # Storage type
		self.level = policy.getString("level") # Level in camera hierarchy
		if policy.exists("map"):
			self.mapFunc = policy.getString("map")
		else:
			self.mapFunc = None
		if policy.exists("standardize"):
			self.stdFunc = policy.getString("standardize")
		else:
			self.stdFunc = None
		if policy.exists("tables"):
			tables = policy.getString("tables")
			self.tables = re.split(r"\s+", tables)
		else:
			self.tables = None
		if policy.exists("query"):
			self.query = policy.getString("query")
		else:
			self.query = None
		if policy.exists("lookup"):
			self.lookupFunc = policy.getString("lookup")
		else:
			self.lookupFunc = None
		return


	def map(self,						# Mapping
			mapper,						# Mapper
			dataId						# Data identifiers
			):
		"""Map from data identifier to a filename."""
		properties = re.findall(r"\%\((\w+)\)", self.template)
		actualId = self.need(mapper, properties, dataId)

		if self.mapFunc is not None:
			func = getattr(mapper, self.mapFunc)
			return func(self, actualId)

		path = os.path.join(self.root, mapper._mapActualToPath(self.template, actualId))
		return ButlerLocation(self.python, self.cpp, self.storage, path, actualId)


	def canStandardize(self				# Mapping
					   ):
		"""Returns whether this data set type can be standardized"""
		# Check for specific standardization function
		if self.stdFunc == "None":
			return False
		return True


	# Standardize data 
	def standardize(self,				# Mapping
					mapper,				# Mapper
					item,				# Item to standardize
					dataId				# Data identifiers
					):
		"""Convert data that's been read in into standard format/type."""
		
		# Check for specific standardization function
		if self.stdFunc is not None:
			if self.stdFunc == "None":
				return item
			func = getattr(mapper, self.stdFunc) # Function for standardization
			return func(self, item, dataId)

		# Default standardization
		return mapper._standardize(self, item, dataId)


	def lookup(self,					# Mapping
			   mapper,					# Mapper
			   properties,				# Properties required
			   dataId					# Data identifiers
			   ):
		"""Lookup a list of data properties in the registry."""
		if self.tables is not None:
			return self._lookupTables(mapper, properties, dataId)
		elif self.query is not None:
			return self._lookupQuery(mapper, properties, dataId)
		elif self.lookupFunc is not None:
			func = getattr(mapper, self.lookupFunc)	# Function for lookup
			return func(self, properties, dataId)
		else:
			raise RuntimeError, "No table or query specified to queryMetadata for %s" % self.type


	def have(self,						# Mapping
			 properties,				# Properties required
			 dataId						# Data identifiers
			 ):
		"""Returns whether the provided data identifier has all
		the properties in the provided list."""
		haveAll = True					# Have everything we need?
		for prop in properties:
			if not dataId.has_key(prop):
				return False
		return True


	def need(self,						# Mapping
			 mapper,					# Mapper
			 properties,				# Properties required
			 dataId,					# Data identifiers
			 refresh=False,				# Refresh values if present?
			 clobber=False				# Clobber existing values?
			 ):
		"""Ensures all properties in the provided list are present in
		the data identifier, looking them up as needed.  This is only
		possible for the case where the data identifies a single
		exposure.
		"""
		
		newId = dataId.copy()
		if self.have(properties, newId):
			return newId
		if not refresh:
			newProps = []					# Properties we don't already have
			for prop in properties:
				if not newId.has_key(prop):
					newProps.append(prop)
			if len(newProps) == 0:
				return newId
		else:
			newProps = properties

		lookups = self.lookup(mapper, newProps, newId)
		return addIdentifiers(lookups, newProps, newId, clobber)


##############################################################################################################
# Private methods
##############################################################################################################

    # Perform a lookup in the registry using the "tables" specification
	def _lookupTables(self,				# Mapping
					  mapper,			# Mapper
					  properties,		# Desired properties
					  dataId			# Data identifiers
					  ):
		where = {}
		values = []
		for k, v in dataId.iteritems():
			where[k] = '?'
			values.append(v)
		return self.registry.executeQuery(properties, self.tables, where, None, values)


	# Perform a lookup in the registry using the "query" specification
	def _lookupQuery(self,				# Mapping
					 mapper,			# Mapper
					 properties,		# Desired properties
					 dataId				# Data identifiers
					 ):
		queryProps = re.findall(r"\%\((\w+)\)", self.query)
		if not self.have(queryProps, dataId):
			raise KeyError, "Data identifier missing dependent properties: %s VS %s" % \
				  (self.query, dataId.keys())
		query = self.query % dataId
		query = re.sub(r"[Ss][Ee][Ll][Ee][Cc][Tt]\s+\*", "SELECT " + ",".join(properties), query)
		cursor = self.registry.conn.execute(query) # XXX dirty
		result = []
		for row in cursor:
			result.append(row)
		return result


# Add the results of a lookup to data identifiers
def addIdentifiers(lookups,			    # Lookups from Mappings.lookup()
				   properties,		    # Properties that were looked up
				   dataId,				# Data identifiers
				   clobber=False		# Clobber existing values?
				   ):
	if len(lookups) != 1:
		raise RuntimeError, "No unique lookup for %s from %s: %d" % (properties, dataId, len(lookups))
	for i, prop in enumerate(properties):
		if clobber or not dataId.has_key(prop):
			dataId[prop] = lookups[0][i]
	return dataId


class CalibrationMapping(Mapping):
	"""CalibrationMapping is a Mapping subclass for calibration-type products.

	The difference is that data properties in the query or template
	can be looked up using the "raw" Mapping in addition to the calibration's.
	"""

	# Perform a lookup in the registry using the "query" specification
	def _lookupQuery(self,				# Mapping
					 mapper,			# Mapper
					 properties,		# Properties to look up
					 dataId				# Data identifiers
					 ):
		queryProps = re.findall(r"\%\((\w+)\)", self.query)
		if not self.have(queryProps, dataId):
			# Try to get them from the mapping for the raw data
			raw = mapper.getMapping("raw")
			dataId = raw.need(mapper, queryProps, dataId, refresh=False, clobber=False)
		return Mapping._lookupQuery(self, mapper, properties, dataId)

	def lookup(self,					# Mapping
			   mapper,					# Mapper
			   properties,				# Properties required
			   dataId					# Data identifiers
			   ):
		if self.query is not None:
			return self._lookupQuery(mapper, properties, dataId)
		else:
			raw = mapper.getMapping("raw")	# The 'raw' mapping
			return raw.lookup(mapper, properties, dataId)

	def need(self,						# Mapping
			 mapper,					# Mapper
			 properties,				# Properties required
			 dataId						# Data identifiers
			 ):
		# Always want to clobber anything extant, because it's a property of
		# the input data, not the calibration data.
		return Mapping.need(self, mapper, properties, dataId, refresh=False, clobber=True)
