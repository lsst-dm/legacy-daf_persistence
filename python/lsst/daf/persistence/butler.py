#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2015 LSST Corporation.
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

"""This module defines the Butler class."""
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.builtins import basestring
from builtins import object

import collections
import copy
import inspect
import json
import os
import weakref

import yaml

from lsst.log import Log
import lsst.pex.policy as pexPolicy
from . import LogicalLocation, ReadProxy, ButlerSubset, ButlerDataRef, Persistence, \
    Storage, Policy, NoResults, Repository, DataId, RepositoryCfg, \
    RepositoryArgs, listify, setify, sequencify, doImport, ButlerComposite, genericAssembler, \
    genericDisassembler


class ButlerCfg(Policy, yaml.YAMLObject):
    """Represents a Butler configuration.

        .. warning::

        cfg is 'wet paint' and very likely to change. Use of it in production
        code other than via the 'old butler' API is strongly discouraged.
    """
    yaml_tag = u"!ButlerCfg"

    def __init__(self, cls, repoCfg):
        super(ButlerCfg, self).__init__({'repoCfg': repoCfg, 'cls': cls})


class RepoData(object):
    """Container object for repository data used by Butler

    Parameters
    ----------
    args - RepositoryArgs
        Arguments used to initialize self.repo
    cfg - RepositoryCfg
        Configuration of repository (this is persisted)
    repo - Repository
        The repository class instance
    tags - set
        The tags that apply to this repository, if any
    """

    def __init__(self, args, cfg, repo, tags):
        self.args = args
        self.cfg = cfg
        self.repo = repo
        self.mode = args.mode
        # self.tags is used to keep track of *all* the applicable tags to the Repo, not just the tags in
        # the cfg (e.g. parents inherit their childrens' tags)
        if not isinstance(tags, set):
            raise RuntimeError("tags passed into RepoData must be in a set.")
        self.tags = tags

    def __reduce__(self):
        return (RepoData, (self.args, self.cfg, self.repo, self.mode, self.tags))

    def __repr__(self):
        return "RepoData(args=%s cfg=%s repo=%s tags=%s" % (self.args, self.cfg, self.repo, self.tags)


class RepoDataContainer(object):
    """Container object for RepoData instances owned by a Butler instance."""

    def __init__(self):
        self.byRepoRoot = collections.OrderedDict()  # {repo root, RepoData}
        self.byCfgRoot = {}                          # {repo cfgRoot, RepoData}
        self._inputs = None
        self._outputs = None
        self._all = None

    def add(self, repoData):
        """Add a RepoData to the container

        Parameters
        ----------
        repoData - RepoData instance to add
        """
        self._inputs = None
        self._outputs = None
        self._all = None
        self.byRepoRoot[repoData.cfg.root] = repoData
        self.byCfgRoot[repoData.args.cfgRoot] = repoData

    def inputs(self):
        """Get a list of RepoData that are used to as inputs to the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with readable repositories, in the order to be used when searching.
        """
        if self._inputs is None:
            self._inputs = [rd for rd in self.byRepoRoot.values() if 'r' in rd.mode]
        return self._inputs

    def outputs(self):
        """Get a list of RepoData that are used to as outputs to the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with writable repositories, in the order to be use when searching.
        """
        if self._outputs is None:
            self._outputs = [rd for rd in self.byRepoRoot.values() if 'w' in rd.mode]
        return self._outputs

    def all(self):
        """Get a list of all RepoData that are used to as by the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with writable repositories, in the order to be use when searching.
        """
        if self._all is None:
            self._all = [rd for rd in self.byRepoRoot.values()]
        return self._all

    def __repr__(self):
        return "%s(\nbyRepoRoot=%r, \nbyCfgRoot=%r, \n_inputs=%r, \n_outputs=%s, \n_all=%s)" % (
            self.__class__.__name__,
            self.byRepoRoot,
            self.byCfgRoot,
            self._inputs,
            self._outputs,
            self._all)


class Butler(object):
    """Butler provides a generic mechanism for persisting and retrieving data using mappers.

    A Butler manages a collection of datasets known as a repository. Each dataset has a type representing its
    intended usage and a location. Note that the dataset type is not the same as the C++ or Python type of the
    object containing the data. For example, an ExposureF object might be used to hold the data for a raw
    image, a post-ISR image, a calibrated science image, or a difference image. These would all be different
    dataset types.

    A Butler can produce a collection of possible values for a key (or tuples of values for multiple keys) if
    given a partial data identifier. It can check for the existence of a file containing a dataset given its
    type and data identifier. The Butler can then retrieve the dataset. Similarly, it can persist an object to
    an appropriate location when given its associated data identifier.

    Note that the Butler has two more advanced features when retrieving a data set. First, the retrieval is
    lazy. Input does not occur until the data set is actually accessed. This allows datasets to be retrieved
    and placed on a clipboard prospectively with little cost, even if the algorithm of a stage ends up not
    using them. Second, the Butler will call a standardization hook upon retrieval of the dataset. This
    function, contained in the input mapper object, must perform any necessary manipulations to force the
    retrieved object to conform to standards, including translating metadata.

    Public methods:

    __init__(self, root, mapper=None, **mapperArgs)

    defineAlias(self, alias, datasetType)

    getKeys(self, datasetType=None, level=None)

    queryMetadata(self, datasetType, format=None, dataId={}, **rest)

    datasetExists(self, datasetType, dataId={}, **rest)

    get(self, datasetType, dataId={}, immediate=False, **rest)

    put(self, obj, datasetType, dataId={}, **rest)

    subset(self, datasetType, level=None, dataId={}, **rest)

    dataRef(self, datasetType, level=None, dataId={}, **rest)

    Initialization:

    The preferred method of initialization is to pass in a RepositoryArgs instance, or a list of
    RepositoryArgs to inputs and/or outputs.

    For backward compatibility: this initialization method signature can take a posix root path, and
    optionally a mapper class instance or class type that will be instantiated using the mapperArgs input
    argument. However, for this to work in a backward compatible way it creates a single repository that is
    used as both an input and an output repository. This is NOT preferred, and will likely break any
    provenance system we have in place.

    Parameters
    ----------
    root - string
        .. note:: Deprecated in 12_0
                  `root` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        A fileysystem path. Will only work with a PosixRepository.
    mapper - string or instance
        .. note:: Deprecated in 12_0
                  `mapper` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        Provides a mapper to be used with Butler.
    mapperArgs - dict
        .. note:: Deprecated in 12_0
                  `mapperArgs` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        Provides arguments to be passed to the mapper if the mapper input arg is a class type to be
        instantiated by Butler.
    inputs - RepositoryArgs or string
        Can be a single item or a list. Provides arguments to load an existing repository (or repositories).
        String is assumed to be a URI and is used as the cfgRoot (URI to the location of the cfg file). (Local
        file system URI does not have to start with 'file://' and in this way can be a relative path).
    outputs - RepositoryArg or string
        Can be a single item or a list. Provides arguments to load one or more existing repositories or create
        new ones. String is assumed to be a URI and as used as the repository root.
    """

    def __init__(self, root=None, mapper=None, inputs=None, outputs=None, **mapperArgs):

        self._initArgs = {'root': root, 'mapper': mapper, 'inputs': inputs, 'outputs': outputs,
                          'mapperArgs': mapperArgs}

        isLegacyRepository = inputs is None and outputs is None

        if root is not None and not isLegacyRepository:
            raise RuntimeError(
                'root is a deprecated parameter and may not be used with the parameters input and output')

        if isLegacyRepository:
            if root is None:
                if hasattr(mapper, 'root'):
                    # in legacy repos, the mapper may be given the root directly.
                    root = mapper.root
                else:
                    # in the past root="None" could be used to mean root='.'
                    root = '.'
            outputs = RepositoryArgs(mode='rw',
                                     root=root,
                                     mapper=mapper,
                                     mapperArgs=mapperArgs)
            outputs.isLegacyRepository = True

        self.datasetTypeAliasDict = {}

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)
        self.log = Log.getLogger("daf.persistence.butler")

        inputs = listify(inputs)
        outputs = listify(outputs)

        # if only a string is passed for inputs or outputs, assumption is that it's a URI;
        # place it in a RepositoryArgs instance; cfgRoot for inputs, root for outputs.
        inputs = [RepositoryArgs(cfgRoot=i) if isinstance(i, basestring) else i for i in inputs]
        outputs = [RepositoryArgs(root=o) if isinstance(o, basestring) else o for o in outputs]

        # set default rw modes on input and output args as needed
        for i in inputs:
            if i.mode is None:
                i.mode = 'r'
        for o in outputs:
            if o.mode is None:
                o.mode = 'w'

        self._repos = RepoDataContainer()

        defaultMapper = self._getDefaultMapper(inputs)

        butlerIOParents = collections.OrderedDict()
        for args in outputs + inputs:
            if 'r' in args.mode:
                butlerIOParents[args.cfgRoot] = args

        for args in outputs:
            self._addRepo(args, inout='out', defaultMapper=defaultMapper, butlerIOParents=butlerIOParents)

        for args in inputs:
            self._addRepo(args, inout='in', butlerIOParents=butlerIOParents)

        self.objectCache = weakref.WeakValueDictionary()


    def _addRepo(self, args, inout, defaultMapper=None, butlerIOParents=None, tags=None):
        """Create a Repository and related infrastructure and add it to the list of repositories.

        Parameters
        ----------
        args - RepositoryArgs
            settings used to create the repository.
        inout - string
            must be 'in' our 'out', indicates how the repository is to be used. Input repos are only read
            from, and output repositories may be read from and/or written to (w/rw of output repos depends on
            args.mode)
        defaultMapper - mapper class or None
            if a default mapper could be inferred from inputs then this will be a class object that can be
            used for any outputs that do not explicitly define their mapper. If None then a mapper class could
            not be inferred and a mapper must be defined by each output.
        butlerIOParents - ordered dict
            The keys are cfgRoot of repoArgs, val is the repoArgs. This is all the explicit input and output
            repositories to the butler __init__ function, it is used when determining what the parents of
            writable repositories are.
        tags - any or list of any
            Any object that can be tested to be the same as the tag in a dataId passed into butler input
            functions. Applies only to input repositories: If tag is specified by the dataId then the repo
            will only be read from used if the tag in the dataId matches a tag used for that repository.
        """
        if butlerIOParents is None:
            butlerIOParents = {}

        tags = copy.copy(setify(tags))
        tags.update(args.tags)

        # If this repository has already been loaded/created, compare the input args to the exsisting repo;
        # if they don't match raise an exception - the caller has to sort this out.
        if args.cfgRoot in self._repos.byCfgRoot:
            repoData = self._repos.byCfgRoot[args.cfgRoot]
            if not repoData.cfg.matchesArgs(args):
                raise RuntimeError("Mismatched repository configurations passed in or loaded:" +
                                   "\n\tnew args:%s" +
                                   "\n\texisting repoData:%s" %
                                   (args, repoData))
            repoData.tags.update(setify(tags))
        else:
            # If we are here, the repository is not yet loaded by butler. Do the loading:

            # Verify mode is legal (must be writeable for outputs, readable for inputs).
            # And set according to the default value if needed.
            if inout == 'out':
                if 'w' not in args.mode:
                    raise RuntimeError('Output repositories must be writable.')
            elif inout == 'in':
                if 'r' not in args.mode:
                    raise RuntimeError('Input repositories must be readable.')
            else:
                raise RuntimeError('Unrecognized value for inout:' % inout)

            # Try to get the cfg.
            # If it exists, verify no mismatch with args.
            # If it does not exist, make the cfg and save the cfg at cfgRoot.
            cfg = Storage.getRepositoryCfg(args.cfgRoot)

            parentsToAdd = []
            if cfg is not None:
                if inout == 'out':
                    # Parents used by this butler instance must match the parents of any existing output
                    # repositories used by this butler. (If there is a configuration change a new output
                    # repository should be created).
                    # IE cfg.parents can have parents that are not passed in as butler inputs or readable
                    # outputs, but the butler may not have readable i/o that is not a parent of an already
                    # existing output.
                    for cfgRoot in butlerIOParents:
                        if cfgRoot not in cfg.parents and cfgRoot != args.cfgRoot:
                            raise RuntimeError(
                                "Existing output repository parents do not match butler's inputs.")
                if not cfg.matchesArgs(args):
                    raise RuntimeError(
                        "Persisted RepositoryCfg and passed-in RepositoryArgs have"
                        " conflicting parameters:\n" + "\t%s\n\t%s", (cfg, args))
                if args.mapperArgs is not None:
                    if cfg.mapperArgs is None:
                        cfg.mapperArgs = args.mapperArgs
                    else:
                        cfg.mapperArgs.update(args.mapperArgs)
                if 'r' in args.mode:
                    parentsToAdd = copy.copy(cfg.parents)
            else:
                if args.mapper is None:
                    if defaultMapper is None:
                        raise RuntimeError(
                            "Could not infer mapper and one not specified in repositoryArgs:%s" % args)
                    args.mapper = defaultMapper
                parents = [cfgRoot for cfgRoot in list(butlerIOParents.keys()) if cfgRoot != args.cfgRoot]
                cfg = RepositoryCfg.makeFromArgs(args, parents)
                Storage.putRepositoryCfg(cfg, args.cfgRoot)

            repo = Repository(cfg)
            self._repos.add(RepoData(args, cfg, repo, tags))
            for parent in parentsToAdd:
                if parent in butlerIOParents:
                    args = butlerIOParents[parent]
                else:
                    args = RepositoryArgs(cfgRoot=parent, mode='r')
                self._addRepo(args=args, inout='in', tags=tags)

    def __repr__(self):
        return 'Butler(datasetTypeAliasDict=%s, repos=%s, persistence=%s)' % (
            self.datasetTypeAliasDict, self._repos, self.persistence)

    @staticmethod
    def _getDefaultMapper(inputs):
        mappers = set()
        for args in inputs:
            if args.mapper is not None:
                mapper = args.mapper
                # if the mapper is:
                # * a string, import it.
                # * a class instance, get its class type
                # * a class, do nothing; use it
                if isinstance(mapper, basestring):
                    mapper = doImport(args.mapper)
                elif not inspect.isclass(mapper):
                    mapper = mapper.__class__
            else:
                cfgRoot = args.cfgRoot
                mapper = Butler.getMapperClass(cfgRoot)
            mappers.add(mapper)

        if len(mappers) == 1:
            return mappers.pop()
        else:
            return None

    @staticmethod
    def getMapperClass(root):
        """posix-only; gets the mapper class at the path specifed by root (if a file _mapper can be found at
        that location or in a parent location.

        As we abstract the storage and support different types of storage locations this method will be
        moved entirely into Butler Access, or made more dynamic, and the API will very likely change."""
        return Storage.getMapperClass(root)

    def defineAlias(self, alias, datasetType):
        """Register an alias that will be substituted in datasetTypes.

        Paramters
        ---------
        alias - str
            The alias keyword. It may start with @ or not. It may not contain @ except as the first character.
        datasetType - str
            The string that will be substituted when @alias is passed into datasetType. It may not contain '@'
        """
        # verify formatting of alias:
        # it can have '@' as the first character (if not it's okay, we will add it) or not at all.
        atLoc = alias.rfind('@')
        if atLoc == -1:
            alias = "@" + str(alias)
        elif atLoc > 0:
            raise RuntimeError("Badly formatted alias string: %s" % (alias,))

        # verify that datasetType does not contain '@'
        if datasetType.count('@') != 0:
            raise RuntimeError("Badly formatted type string: %s" % (datasetType))

        # verify that the alias keyword does not start with another alias keyword,
        # and vice versa
        for key in self.datasetTypeAliasDict:
            if key.startswith(alias) or alias.startswith(key):
                raise RuntimeError("Alias: %s overlaps with existing alias: %s" % (alias, key))

        self.datasetTypeAliasDict[alias] = datasetType

    def getKeys(self, datasetType=None, level=None, tag=None):
        """Get the valid data id keys at or above the given level of hierarchy for the dataset type or the
        entire collection if None. The dict values are the basic Python types corresponding to the keys (int,
        float, str).

        Parameters
        ----------
        datasetType - str
            The type of dataset to get keys for, entire collection if None.
        level - str
            The hierarchy level to descend to. None if it should not be restricted. Use an empty string if the
            mapper should lookup the default level.
        tags - any, or list of any
            Any object that can be tested to be the same as the tag in a dataId passed into butler input
            functions. Applies only to input repositories: If tag is specified by the dataId then the repo
            will only be read from used if the tag in the dataId matches a tag used for that repository.

        Returns
        -------
        Returns a dict. The dict keys are the valid data id keys at or above the given level of hierarchy for
        the dataset type or the entire collection if None. The dict values are the basic Python types
        corresponding to the keys (int, float, str).
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)

        keys = None
        tag = setify(tag)
        for repoData in self._repos.inputs():
            if not tag or len(tag.intersection(repoData.tags)) > 0:
                keys = repoData.repo.getKeys(datasetType, level)
                # An empty dict is a valid "found" condition for keys. The only value for keys that should
                # cause the search to continue is None
                if keys is not None:
                    break
        return keys

    def queryMetadata(self, datasetType, format=None, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.

        Parameters
        ----------
        datasetType - str
            The type of dataset to inquire about.
        key - str
            A key giving the level of granularity of the inquiry.
        format - str, tuple
            An optional key or tuple of keys to be returned.
        dataId - DataId, dict
            The partial data id.
        **rest -
            Keyword arguments for the partial data id.

        Returns
        -------
        A list of valid values or tuples of valid values as specified by the format (defaulting to the same as
        the key) at the key's level of granularity.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)

        if format is None:
            format = (key,)
        else:
            format = sequencify(format)

        tuples = None
        for repoData in self._repos.inputs():
            if not dataId.tag or len(dataId.tag.intersection(repoData.tags)) > 0:
                tuples = repoData.repo.queryMetadata(datasetType, format, dataId)
                if tuples:
                    break

        if not tuples:
            return []

        if len(format) == 1:
            ret = []
            for x in tuples:
                try:
                    ret.append(x[0])
                except TypeError:
                    ret.append(x)
            return ret

        return tuples

    def datasetExists(self, datasetType, dataId={}, **rest):
        """Determines if a dataset file exists.

        Parameters
        ----------
        datasetType - str
            The type of dataset to inquire about.
        dataId - DataId, dict
            The data id of the dataset.
        **rest keyword arguments for the data id.

        Returns
        -------
        exists - bool
            True if the dataset exists or is non-file-based.
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)

        location = None
        for repoData in self._repos.inputs():
            if not dataId.tag or len(dataId.tag.intersection(repoData.tags)) > 0:
                location = repoData.repo.map(datasetType, dataId)
                if location:
                    break

        if location is None:
            return False

        additionalData = location.getAdditionalData()
        storageName = location.getStorageName()
        if storageName in ('BoostStorage', 'FitsStorage', 'PafStorage',
                           'PickleStorage', 'ConfigStorage', 'FitsCatalogStorage'):
            locations = location.getLocations()
            for locationString in locations:
                logLoc = LogicalLocation(locationString, additionalData).locString()
                if storageName == 'FitsStorage':
                    # Strip off directives for cfitsio (in square brackets, e.g., extension name)
                    bracket = logLoc.find('[')
                    if bracket > 0:
                        logLoc = logLoc[:bracket]
                if not os.path.exists(logLoc):
                    return False
            return True
        self.log.warn("datasetExists() for non-file storage %s, dataset type=%s, keys=%s",
                      storageName, datasetType, str(dataId))
        return True

    def _locate(self, datasetType, dataId, write):
        """Get one or more ButlerLocations and/or ButlercComposites.

        Parameters
        ----------
        datasetType : string
            The datasetType that is being searched for. The datasetType may be followed by a dot and
            a component name (component names are specified in the policy). IE datasetType.componentName

        dataId : dict or DataId class instance
            The dataId

        write : bool
            True if this is a search to write an object. False if it is a search to read an object. This
            affects what type (an object or a container) is returned.

        Returns
        -------
        If write is False, will return either a single object or None. If write is True, will return a list
        (which may be empty)
        """
        repos = self._repos.outputs() if write else self._repos.inputs()
        locations = []
        for repoData in repos:
            # enforce dataId & repository tags when reading:
            if not write and dataId.tag and len(dataId.tag.intersection(repoData.tags)) == 0:
                continue
            components = datasetType.split('.')
            datasetType = components[0]
            components = components[1:]
            location = repoData.repo.map(datasetType, dataId, write=write)
            if location is None:
                continue
            location.datasetType = datasetType  # todo is there a better way than monkey patching here?
            if len(components) > 0:
                if not isinstance(location, ButlerComposite):
                    raise RuntimeError("The location for a dotted datasetType must be a composite.")
                # replace the first component name with the datasetType
                components[0] = location.componentInfo[components[0]].datasetType
                # join components back into a dot-delimited string
                datasetType = '.'.join(components)
                location = self._locate(datasetType, dataId, write)
                # if a cmponent location is not found, we can not continue with this repo, move to next repo.
                if location is None:
                    break
            # if reading, only one location is desired.
            if location:
                if not write:
                    return location
                else:
                    try:
                        locations.extend(location)
                    except TypeError:
                        locations.append(location)
        if not write:
            return None
        return locations

    def get(self, datasetType, dataId=None, immediate=True, **rest):
        """Retrieves a dataset given an input collection data id.

        Parameters
        ----------
        datasetType - str
            The type of dataset to retrieve.
        dataId - dict
            The data id.
        immediate - bool
            If False use a proxy for delayed loading.
        **rest
            keyword arguments for the data id.

        Returns
        -------
            An object retrieved from the dataset (or a proxy for one).
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)

        location = self._locate(datasetType, dataId, write=False)
        if location is None:
            raise NoResults("No locations for get:", datasetType, dataId)
        self.log.debug("Get type=%s keys=%s from %s", datasetType, dataId, str(location))

        if isinstance(location, ButlerComposite):
            for name, componentInfo in location.componentInfo.items():
                if componentInfo.subset:
                    subset = self.subset(datasetType=componentInfo.datasetType, dataId=location.dataId)
                    componentInfo.obj = [obj.get() for obj in subset]
                else:
                    obj = self.get(componentInfo.datasetType, location.dataId, immediate=True)
                    componentInfo.obj = obj
                assembler = location.assembler or genericAssembler
            obj = assembler(dataId=location.dataId, componentInfo=location.componentInfo, cls=location.python)
            return obj

# <<<<<<< HEAD
#                 obj = self.get(componentInfo.datasetType, location.dataId, immediate=True)
#                 componentInfo.obj = obj
#             assembler = location.assembler or genericAssembler
#             obj = assembler(dataId=location.dataId, componentInfo=location.componentInfo, cls=location.python)
#             return obj
# =======
#                 if componentInfo.subset:
#                     subset = self.subset(datasetType=componentInfo.datasetType, dataId=location.dataId)
#                     componentInfo.obj = [obj.get() for obj in subset]
#                 else:
#                     componentObj = self.get(componentInfo.datasetType, location.dataId, immediate=True)
#                     componentInfo.obj = componentObj
#             assembler = location.assembler if location.assembler else genericAssembler
#             compositeObj = assembler(dataId=location.dataId, componentInfo=location.componentInfo,
#                                      cls=location.python)
#             return compositeObj
# >>>>>>> f5a2611... support for getting  composite subset

        if location.datasetType and hasattr(location.mapper, "bypass_" + location.datasetType):
            # this type loader block should get moved into a helper someplace, and duplciations removed.
            pythonType = location.getPythonType()
            if pythonType is not None:
                if isinstance(pythonType, basestring):
                    pythonType = doImport(pythonType)
            bypassFunc = getattr(location.mapper, "bypass_" + location.datasetType)
            callback = lambda: bypassFunc(location.datasetType, pythonType, location, dataId)
        else:
            callback = lambda: self._read(location)
        if location.mapper.canStandardize(location.datasetType):
            innerCallback = callback
            callback = lambda: location.mapper.standardize(location.datasetType, innerCallback(), dataId)
        if immediate:
            return callback()
        return ReadProxy(callback)

    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        """Persists a dataset given an output collection data id.

        Parameters
        ----------
        obj -
            The object to persist.
        datasetType - str
            The type of dataset to persist.
        dataId - dict
            The data id.
        doBackup - bool
            If True, rename existing instead of overwriting.
            WARNING: Setting doBackup=True is not safe for parallel processing, as it may be subject to race
            conditions.
        **rest
            Keyword arguments for the data id.
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)

        for location in self._locate(datasetType, dataId, write=True):
            if isinstance(location, ButlerComposite):
                disassembler = location.disassembler if location.disassembler else genericDisassembler
                disassembler(obj=obj, dataId=location.dataId, componentInfo=location.componentInfo)
                for name, info in location.componentInfo.items():
                    if not info.inputOnly:
                        self.put(info.obj, info.datasetType, location.dataId, doBackup=doBackup)
            else:
                if doBackup:
                    location.getRepository().backup(location.datasetType, dataId)
                location.getRepository().write(location, obj)

    def subset(self, datasetType, level=None, dataId={}, **rest):
        """Return complete dataIds for a dataset type that match a partial (or empty) dataId.

        Given a partial (or empty) dataId specified in dataId and **rest, find all datasets that match the
        dataId.  Optionally restrict the results to a given level specified by a dataId key (e.g. visit or
        sensor or amp for a camera).  Return an iterable collection of complete dataIds as ButlerDataRefs.
        Datasets with the resulting dataIds may not exist; that needs to be tested with datasetExists().

        Parameters
        ----------
        datasetType - str
            The type of dataset collection to subset
        level - str
            The level of dataId at which to subset. Use an empty string if the mapper should look up the
            default level.
        dataId - dict
            The data id.
        **rest
            Keyword arguments for the data id.

        Returns
        -------
        subset - ButlerSubset
            Collection of ButlerDataRefs for datasets matching the data id.

        Examples
        -----------
        To print the full dataIds for all r-band measurements in a source catalog
        (note that the subset call is equivalent to: `butler.subset('src', dataId={'filter':'r'})`):

        >>> subset = butler.subset('src', filter='r')
        >>> for data_ref in subset: print(data_ref.dataId)
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)

        # Currently expected behavior of subset is that if specified level is None then the mapper's default
        # level should be used. Convention for level within Butler is that an empty string is used to indicate
        # 'get default'.
        if level is None:
            level = ''

        dataId = DataId(dataId)
        dataId.update(**rest)
        return ButlerSubset(self, datasetType, level, dataId)

    def dataRef(self, datasetType, level=None, dataId={}, **rest):
        """Returns a single ButlerDataRef.

        Given a complete dataId specified in dataId and **rest, find the unique dataset at the given level
        specified by a dataId key (e.g. visit or sensor or amp for a camera) and return a ButlerDataRef.

        Parameters
        ----------
        datasetType - str
            The type of dataset collection to reference
        level - str
            The level of dataId at which to reference
        dataId - dict
            The data id.
        **rest
            Keyword arguments for the data id.

        Returns
        -------
        dataRef - ButlerDataRef
            ButlerDataRef for dataset matching the data id
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        subset = self.subset(datasetType, level, dataId, **rest)
        if len(subset) != 1:
            raise RuntimeError("No unique dataset for: Dataset type:%s Level:%s Data ID:%s Keywords:%s" %
                               (str(datasetType), str(level), str(dataId), str(rest)))
        return ButlerDataRef(subset, subset.cache[0])

    def _read(self, location):
        """Unpersist an object using data inside a butlerLocation object.

        A weakref to loaded objects is cached here. If the object specified by the butlerLocaiton has been
        loaded before and still exists then the object will not be re-read. A ref to the already-existing
        object will be returned instead.

        Parameters
        ----------
        location - ButlerLocation
            A butlerLocation instance populated with data needed to read the object.

        Returns
        -------
        object - an instance of the object specified by the butlerLoction.
            The object specified by the butlerLocation will either be loaded from persistent storage or will
            be fetched from the object cache (if it has already been read before).
        """
        def hasher(butlerLocation):
            """Hash a butler location for use as a key in the object cache.

            This requires that the dataId that was used to find the location is set in the usedDataId
            parameter. If this is not set, the dataId that was used to do the mapping is not known and we
            can't create a complete hash for comparison of like-objects.
            """
            if butlerLocation.usedDataId is None:
                return None
            return hash((butlerLocation.storageName, id(butlerLocation.mapper), id(butlerLocation.storage),
                         tuple(butlerLocation.locationList), repr(sorted(butlerLocation.usedDataId.items())),
                         butlerLocation.datasetType))

        locationHash = hasher(location)
        results = self.objectCache.get(locationHash, None) if locationHash is not None else None
        if not results:
            self.log.debug("Starting read from %s", location)
            results = location.repository.read(location)
            if len(results) == 1:
                results = results[0]
            self.log.debug("Ending read from %s", location)
            try:
                self.objectCache[locationHash] = results
            except TypeError:
                # some object types (e.g. builtins, like list) do not support weakref, and will raise a
                # TypeError when we try to create the weakref. This is ok, we simply will not keep those
                # types of objects in the cache.
                pass
        return results

    def __reduce__(self):
        ret = (_unreduce, (self._initArgs, self.datasetTypeAliasDict))
        return ret

    def _resolveDatasetTypeAlias(self, datasetType):
        """Replaces all the known alias keywords in the given string with the alias value.

        Parameters
        ----------
        datasetType - str
            A datasetType string to search & replace on

        Returns
        -------
        datasetType - str
            The de-aliased string
        """
        for key in self.datasetTypeAliasDict:
            # if all aliases have been replaced, bail out
            if datasetType.find('@') == -1:
                break
            datasetType = datasetType.replace(key, self.datasetTypeAliasDict[key])

        # If an alias specifier can not be resolved then throw.
        if datasetType.find('@') != -1:
            raise RuntimeError("Unresolvable alias specifier in datasetType: %s" % (datasetType))

        return datasetType


def _unreduce(initArgs, datasetTypeAliasDict):
    mapperArgs = initArgs.pop('mapperArgs')
    initArgs.update(mapperArgs)
    butler = Butler(**initArgs)
    butler.datasetTypeAliasDict = datasetTypeAliasDict
    return butler
