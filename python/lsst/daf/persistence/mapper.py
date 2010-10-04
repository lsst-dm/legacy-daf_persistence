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
from lsst.daf.persistence import ButlerLocation, LogicalLocation, Mapping, CalibrationMapping
import lsst.daf.butlerUtils as butlerUtils
import lsst.daf.base as dafBase
import lsst.afw.image as afwImage
import lsst.afw.cameraGeom as afwCameraGeom
import lsst.afw.cameraGeom.utils as cameraGeomUtils
import lsst.afw.image.utils as imageUtils
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy

"""This module defines the Mapper base class."""

class Mapper(object):
	
	"""Mapper is a base class for all data mappers.  This provides an
	abstraction layer between the data on disk and the code.

	Public methods: getKeys, queryMetadata, getDatasetTypes, map,
	canStandardize, standardize, getMapping

	Mappers for specific data sources (e.g., CFHT Megacam, LSST
	simulations, etc.) should inherit this class.  Such subclasses
	should set in __init__:

	keys: List of keys that can be used in data IDs.

	filterMap: Dict with mapping from data's filter name (e.g.,
	"r.12345") to the code's filter name (e.g., "r")

	filterIdMap: Mapping from the code's filter name (e.g., "r") to an
	integer identifier.

	The following methods must be provided by the subclass:

	_transformId(self, dataId): transformation of a data identifier
	from colloquial usage (e.g., "ccdname") to proper/actual usage
	(e.g., "ccd").

	_extractDetectorName(self, dataId): returns the detector name
	(e.g., "CFHT 21", "R:1,2 S:3,4").

	The mapper's behaviours are largely specified by the policy file,
	which consists of:

	camera (string): Name of camera geometry policy file

	defects (string): Path to defects registry

	filters (string): Name of filters policy file

	exposures (policy): Exposure mappings (e.g., "raw", "postISR")

	calibrations (policy): Calibration mappings (e.g., "bias", "flat")

	The 'exposures' and 'calibrations' policies consist of mappings
	(see Mappings class).

	Functions to map (provide a path to the data from some data
	identifiers) and standardize (convert data into some standard
	format or type) may be provided in the subclass, and will be
	called provided they are specified in the policy.
	"""

	def __init__(self,					# Mapper
				 policy=None,			# Default policy or policy filename
				 module=None,			# Module name, for policy lookup
				 policyDir=None,		# Policy directory relative to module directory
				 root=".",				# Root directory for data
				 registry=None,			# Registry with data
				 calibRoot=None,	    # Root directory for calibrations
				 calibRegistry=None		# Registry with calibrations
				 ):
		self.log = pexLog.Log(pexLog.getDefaultLog(), "Mapper")

		# Camera policy setup
		if policy is None:
			self.policy = pexPolicy.Policy()
		elif isinstance(policy, pexPolicy.Policy):
			self.policy = policy
		else:
			polFile = pexPolicy.DefaultPolicyFile(module, policy, policyDir)
			self.policy = pexPolicy.Policy.createPolicy(polFile, polFile.getRepositoryPath())

		# Dictionary
		dictFile = pexPolicy.DefaultPolicyFile("daf_persistence", "MapperDictionary.paf", "policy")
		dictPolicy = pexPolicy.Policy.createPolicy(dictFile, dictFile.getRepositoryPath()) # Dictionary
		self.policy.mergeDefaults(dictPolicy)

		# Root directories
		self.root = root
		if self.policy.exists('root'):
			self.root = self.policy.getString('root')
		self.calibRoot = calibRoot
		if self.policy.exists('calibRoot'):
			self.calibRoot = self.policy.getString('calibRoot')
		if self.calibRoot is None:
			self.calibRoot = self.root
		# Do any location substitutions
		self.root = LogicalLocation(self.root).locString()
		self.calibRoot = LogicalLocation(self.calibRoot).locString()
		if not os.path.exists(self.root):
			self.log.log(pexLog.Log.WARN,
						 "Root directory not found: %s" % (root,))
		if not os.path.exists(self.calibRoot):
			self.log.log(pexLog.Log.WARN,
						 "Calibration root directory not found: %s" % (calibRoot,))

		# Registries
		self.registry = self._setupRegistry("registry", registry, "registryPath", root)
		self.calibRegistry = self._setupRegistry("calibRegistry", calibRegistry,
												 "calibRegistryPath", calibRoot)

		# Sub-dictionary (for exposure/calibration types)
		mappingFile = pexPolicy.DefaultPolicyFile("daf_persistence", "MappingDictionary.paf", "policy")
		mappingPolicy = pexPolicy.Policy.createPolicy(mappingFile, mappingFile.getRepositoryPath())

		# Mappings
		self.mappings = dict()
		if self.policy.exists("exposures"):
			exposures = self.policy.getPolicy("exposures") # List of exposure types
			for type in exposures.names(True):
				subPolicy = exposures.get(type)
				if not isinstance(subPolicy, pexPolicy.Policy):
					raise RuntimeError, "Exposure type %s is not a policy" % type
				subPolicy.mergeDefaults(mappingPolicy)
				self.mappings[type] = Mapping(mapper=self, policy=subPolicy, type=type,
											  registry=self.registry, root=self.root)
		if self.policy.exists("calibrations"):
			calibs = self.policy.getPolicy("calibrations") # List of calibration types
			for type in calibs.names(True):
				subPolicy = calibs.get(type)
				if not isinstance(subPolicy, pexPolicy.Policy):
					raise RuntimeError, "Calibration type %s is not a policy" % type
				subPolicy.mergeDefaults(mappingPolicy)
				self.mappings[type] = CalibrationMapping(mapper=self, policy=subPolicy, type=type,
														 registry=self.calibRegistry, root=self.calibRoot)

		# Subclass should override these!
		self.keys = []
		self.filterMap = {}
		self.filterIdMap = {}

		# Camera geometry
		self.cameraPolicyLocation = None
		self.camera = None
		if self.policy.exists('camera'):
			self.cameraPolicyLocation = self.policy.getString('camera')
			cameraPolicy = cameraGeomUtils.getGeomPolicy(self.cameraPolicyLocation, module=module,
														 directory=policyDir)
			self.camera = cameraGeomUtils.makeCamera(cameraPolicy)

		# Defect registry
		self.defectRegistry = None
		if self.policy.exists('defects'):
			self.defectPath = self.policy.getString('defects')
			defectRegistryLocation = os.path.join(
					self.defectPath, "defectRegistry.sqlite3")
			self.defectRegistry = \
					butlerUtils.Registry.create(defectRegistryLocation)

		# Filters
		if self.policy.exists('filters'):
			filterPolicyLocation = self.policy.getString('filters')
			filterPolicyFile = pexPolicy.DefaultPolicyFile(module, filterPolicyLocation, policyDir)
			filterPolicy = pexPolicy.Policy.createPolicy(filterPolicyFile,
														 filterPolicyFile.getRepositoryPath())
			imageUtils.defineFiltersFromPolicy(filterPolicy, reset=True)


	def getKeys(self):
		"""Return supported keys"""
		return self.keys

	def queryMetadata(self,				# Mapper
					  type,				# Data set type
					  key,				# Level of granularity of the inquiry (no idea what that means!)
					  format,			# Properties of interest, to be returned
					  dataId			# Data specifiers
					  ):
		"""Return possible values for keys given a partial data id."""
		actualId = self._transformId(dataId)
		return self.getMapping(type).lookup(self, format, actualId)

	def getDatasetTypes(self):
		"""Return a list of the mappable dataset types."""
		return self.mappings.keys()

	def map(self,						# The Mapper
			type,						# Type of data
			dataId						# Data specifiers
			):
		"""Map a data id using the mapping method for its dataset type."""
		actualId = self._transformId(dataId)
		if type == "camera":
			return ButlerLocation("lsst.afw.cameraGeom.Camera", "Camera",
								  "PafStorage", self.cameraPolicyLocation, actualId)
		return self.getMapping(type).map(self, actualId)

	def canStandardize(self, type):
		"""Return true if this mapper can standardize an object of the given
		dataset type."""
		try:
			mapping = self.getMapping(type)
		except KeyError:
			return False
		return mapping.canStandardize()

	def standardize(self,			   # The Mapper
					type,			   # Type of data
					item,			   # The thing to be standardized
					dataId			   # Data specifiers
					):
		"""Standardize an object using the standardization method for its data
		set type, if it exists."""
		actualId = self._transformId(dataId)
		if type == "camera":
			return cameraGeomUtils.makeCamera(cameraGeomUtils.getGeomPolicy(item))
		return self.getMapping(type).standardize(self, item, dataId)

	def getMapping(self,				# Mapper
				   type					# Data set type
				   ):
		"""Return the appropriate mapping for the data set type
		"""
		return self.mappings[type]

###############################################################################
#
# Utility functions
#
###############################################################################

    # Set up a sqlite3 registry, trying a bunch of possible paths
	def _setupRegistry(self,			# Mapper
					   name,			# Name of registry
					   path,			# Path for registry
					   policyName,		# Name of path item in policy
					   root				# Root directory to try
					   ):
		if path is None and self.policy.exists(policyName):
			path = LogicalLocation(self.policy.getString(policyName)).locString()
			if not os.path.exists(path):
				self.log.log(pexLog.Log.WARN,
							 "Unable to locate registry at path: %s" % path)
				path = None
		if path is None and root is not None:
			path = os.path.join(root, "%s.sqlite3" % name)
			if not os.path.exists(path):
				self.log.log(pexLog.Log.WARN,
							 "Unable to locate %s registry in root: %s" % (name, path))
				path = None
		if path is None:
			path = "%s.sqlite3" % name
			if not os.path.exists(path):
				self.log.log(pexLog.Log.WARN,
							 "Unable to locate %s registry in current dir: %s" % (name, path))
				path = None
		if path is not None:
			self.log.log(pexLog.Log.INFO,
						 "Loading %s registry from %s" % (name, path))
			return butlerUtils.Registry.create(path)
		else:
			# TODO Try a FsRegistry(root)
			self.log.log(pexLog.Log.WARN,
						 "No registry loaded; proceeding without one")
			return None

	# Transform an id from camera-specific usage to standard form (e.g,. ccdname --> ccd)
	# This function must be provided by the subclass
	def _transformId(self, dataId):
		raise RuntimeError, "No _transformId() function specified"

	# Convert a template path to an actual path, using the actual data identifiers
	# The subclass may (or may not) want to override this
	def _mapActualToPath(self,			# The Mapper
						 template,		# Template path
						 actualId		# Identifiers for the data
						 ):
		return template % actualId

	# Extract detetctor name from the dataId
	# This function must be provided by the subclass
	def _extractDetectorName(self, dataId):
		raise RuntimeError, "No _extractDetectorName() function specified"

	def _extractAmpId(self, dataId):
		return (self._extractDetectorName(dataId),
				int(dataId['amp']), 0)

	def _setAmpDetector(self, mapping, item, dataId):
		ampId = self._extractAmpId(dataId)
		detector = cameraGeomUtils.findAmp(
				self.camera, afwCameraGeom.Id(ampId[0]), ampId[1], ampId[2])
		self._addDefects(dataId, amp=detector)
		item.setDetector(detector)

	def _setCcdDetector(self, type, item, dataId):
		ccdId = self._extractDetectorName(dataId)
		detector = cameraGeomUtils.findCcd(self.camera, afwCameraGeom.Id(ccdId))
		self._addDefects(dataId, ccd=detector)
		item.setDetector(detector)

	def _setFilter(self, mapping, item, dataId):
		md = item.getMetadata()
		filterName = None
		if md.exists("FILTER"):
			filterName = item.getMetadata().get("FILTER").strip()
			if self.filterMap.has_key(filterName):
				filterName = self.filterMap[filterName]
		if filterName is None:
			actualId = mapping.need(self, ['filter'], dataId)
			filterName = actualId['filter']
		filter = afwImage.Filter(filterName)
		item.setFilter(filter)

	def _setTimes(self, mapping, item, dataId):
		md = item.getMetadata()
		calib = item.getCalib()
		if md.exists("EXPTIME"):
			expTime = md.get("EXPTIME")
			calib.setExptime(expTime)
			md.remove("EXPTIME")
		else:
			expTime = calib.getExptime()
		if md.exists("MJD-OBS"):
			obsStart = dafBase.DateTime(md.get("MJD-OBS"),
					dafBase.DateTime.MJD, dafBase.DateTime.UTC)
			obsMidpoint = obsStart.nsecs() + long(expTime * 1000000000L / 2)
			calib.setMidTime(dafBase.DateTime(obsMidpoint))

	# Default standardization function
	def _standardize(self,				# Mapper
					 mapping,			# Mapping
					 item,				# Item to standardize
					 dataId				# Data identifiers
					 ):
		stripFits(item.getMetadata())

		if mapping.level.lower() == "amp":
			self._setAmpDetector(mapping, item, dataId)
		elif mapping.level.lower() == "ccd":
			self._setCcdDetector(mapping, item, dataId)

		if not isinstance(mapping, CalibrationMapping):
			self._setTimes(mapping, item, dataId)
			self._setFilter(mapping, item, dataId)
		elif mapping.type in ['flat', 'fringe']:
			self._setFilter(mapping, item, dataId)
		return item

	def _defectLookup(self, dataId, ccdSerial):
		if self.defectRegistry is None:
			return None

		rows = self.registry.executeQuery(("taiObs",), ("raw_visit",),
				{"visit": "?"}, None, (dataId['visit'],))
		if len(rows) == 0:
			return None
		assert len(rows) == 1
		taiObs = rows[0][0]

		rows = self.defectRegistry.executeQuery(("path",), ("defect",),
				{"ccdSerial": "?"},
				("DATETIME(?)", "DATETIME(validStart)", "DATETIME(validEnd)"),
				(ccdSerial, taiObs))
		if not rows or len(rows) == 0:
			return None
		assert len(rows) == 1
		return os.path.join(self.defectPath, rows[0][0])

	def _addDefects(self, dataId, amp=None, ccd=None):
		if ccd is None:
			ccd = afwCameraGeom.cast_Ccd(amp.getParent())
		if len(ccd.getDefects()) > 0:
			# Assume we have loaded them properly already
			return
		defectFits = self._defectLookup(dataId, ccd.getId().getSerial())
		if defectFits is not None:
			defectDict = cameraGeomUtils.makeDefectsFromFits(defectFits)
			ccdDefects = None
			for k in defectDict.keys():
				if k == ccd.getId():
					ccdDefects = defectDict[k]
					break
			if ccdDefects is None:
				raise RuntimeError, "No defects for ccd %s in %s" % \
						(str(ccd.getId()), defectFits)
			ccd.setDefects(ccdDefects)

def stripFits(propertySet):
	for kw in ("SIMPLE", "BITPIX", "EXTEND", "NAXIS", "NAXIS1", "NAXIS2",
			"BSCALE", "BZERO"):
		if propertySet.exists(kw):
			propertySet.remove(kw)
