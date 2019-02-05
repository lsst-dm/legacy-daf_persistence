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
from builtins import str, super
from past.builtins import basestring
from builtins import object

import copy
import inspect

import yaml

from lsst.log import Log
from . import ReadProxy, ButlerSubset, ButlerDataRef, \
    Storage, Policy, NoResults, Repository, DataId, RepositoryCfg, \
    RepositoryArgs, listify, setify, sequencify, doImport, ButlerComposite, genericAssembler, \
    genericDisassembler, PosixStorage, ParentsMismatch

preinitedMapperWarning = ("Passing an instantiated mapper into " +
                          "Butler.__init__ will prevent Butler from passing " +
                          "parentRegistry or repositoryCfg information to " +
                          "the mapper, which is done only at init time. " +
                          "It is better to pass a importable string or " +
                          "class object.")


class ButlerCfg(Policy, yaml.YAMLObject):
    """Represents a Butler configuration.

        .. warning::

        cfg is 'wet paint' and very likely to change. Use of it in production
        code other than via the 'old butler' API is strongly discouraged.
    """
    yaml_tag = u"!ButlerCfg"

    def __init__(self, cls, repoCfg):
        super().__init__({'repoCfg': repoCfg, 'cls': cls})


class RepoData(object):
    """Container object for repository data used by Butler

    Parameters
    ----------
    args : RepositoryArgs
        The arguments that are used to find or create the RepositoryCfg.
    role : string
        "input", "output", or "parent", indicating why Butler loaded this repository.
        * input: the Repository was passed as a Butler input.
        * output: the Repository was passed as a Butler output.
        * parent: the Repository was specified in the RepositoryCfg parents list of a readable repository.

    Attributes
    ----------
    cfg: RepositoryCfg
        The configuration for the Repository.

    _cfgOrigin : string
        "new", "existing", or "nested". Indicates the origin of the repository and its RepositoryCfg:
        * new: it was created by this instance of Butler, and this instance of Butler will generate the
          RepositoryCfg file.
        * existing: it was found (via the root or cfgRoot argument)
        * nested: the full RepositoryCfg was nested in another RepositoryCfg's parents list (this can happen
          if parameters of an input specified by RepositoryArgs or dict does not entirely match an existing
          RepositoryCfg).

    cfgRoot : string
        Path or URI to the location of the RepositoryCfg file.

    repo : lsst.daf.persistence.Repository
        The Repository class instance.

    parentRepoDatas : list of RepoData
        The parents of this Repository, as indicated this Repository's RepositoryCfg. If this is a new
        Repository then these are the inputs to this Butler (and will be saved in the RepositoryCfg). These
        RepoData objects are not owned by this RepoData, these are references to peer RepoData objects in the
        Butler's RepoDataContainer.

    isV1Repository : bool
        True if this is an Old Butler repository. In this case the repository does not have a RepositoryCfg
        file. It may have a _mapper file and may have a _parent symlink. It will never be treated as a "new"
        repository, i.e. even though there is not a RepositoryCfg file, one will not be generated.
        If False, this is a New Butler repository and is specified by RepositoryCfg file.

    tags : set
        These are values that may be used to restrict the search of input repositories. Details are available
        in the RepositoryArgs and DataId classes.

    role : string
        "input", "output", or "parent", indicating why Butler loaded this repository.
        * input: the Repository was passed as a Butler input.
        * output: the Repository was passed as a Butler output.
        * parent: the Repository was specified in the RepositoryCfg parents list of a readable repository.

    _repoArgs : RepositoryArgs
        Contains the arguments that were used to specify this Repository.
    """

    def __init__(self, args, role):
        self.cfg = None
        self._cfgOrigin = None
        self.cfgRoot = None
        self.repo = None
        self.parentRepoDatas = []
        self.isV1Repository = False
        self.tags = set()
        self.role = role
        self.parentRegistry = None
        self._repoArgs = args

    @property
    def repoArgs(self):
        return self._repoArgs

    @property
    def repoData(self):
        return self

    def __repr__(self):
        return ("{}(id={},"
                "repoArgs={}"
                "cfg={!r},"
                "cfgOrigin={},"
                "cfgRoot={}," +
                "repo={},"
                "parentRepoDatas={}," +
                "isV1Repository={},"
                "role={}," +
                "parentRegistry={})").format(
                    self.__class__.__name__,
                    id(self),
                    self.repoArgs,
                    self.cfg,
                    self.cfgOrigin,
                    self.cfgRoot,
                    self.repo,
                    [id(p) for p in self.parentRepoDatas],
                    self.isV1Repository,
                    self.role,
                    self.parentRegistry)

    def setCfg(self, cfg, origin, root, isV1Repository):
        """Set information about the cfg into the RepoData

        Parameters
        ----------
        cfg : RepositoryCfg
            The RepositoryCfg for the repo.
        origin : string
            'new', 'existing', or 'nested'
        root : string
            URI or absolute path to the location of the RepositoryCfg.yaml file.

        Returns
        -------
        None
        """
        if origin not in ('new', 'existing', 'nested'):
            raise RuntimeError("Invalid value for origin:{}".format(origin))
        self.cfg = cfg
        self._cfgOrigin = origin
        self.cfgRoot = root
        self.isV1Repository = isV1Repository

    @property
    def cfgOrigin(self):
        return self._cfgOrigin

    @property
    def isNewRepository(self):
        return self.cfgOrigin == 'new'

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, val):
        if val not in ('input', 'output', 'parent'):
            raise RuntimeError("Invalid value for role: {}".format(val))
        self._role = val

    def getParentRepoDatas(self, context=None):
        """Get the parents & grandparents etc of this repo data, in depth-first search order.

        Duplicate entries will be removed in cases where the same parent appears more than once in the parent
        graph.

        Parameters
        ----------
        context : set, optional
            Users should typically omit context and accept the default argument. Context is used to keep a set
            of known RepoDatas when calling this function recursively, for duplicate elimination.

        Returns
        -------
        list of RepoData
            A list of the parents & grandparents etc of a given repo data, in depth-first search order.
        """
        if context is None:
            context = set()
        parents = []
        if id(self) in context:
            return parents
        context.add(id(self))
        for parent in self.parentRepoDatas:
            parents.append(parent)
            parents += parent.getParentRepoDatas(context)
        return parents

    def addParentRepoData(self, parentRepoData):
        self.parentRepoDatas.append(parentRepoData)

    def addTags(self, tags):
        self.tags = self.tags.union(tags)


class RepoDataContainer(object):
    """Container object for RepoData instances owned by a Butler instance.

        Parameters
        ----------
        repoDataList : list of RepoData
            repoData - RepoData instance to add
    """

    def __init__(self, repoDataList):
        self._inputs = None
        self._outputs = None
        self._all = repoDataList
        self._buildLookupLists()

    def inputs(self):
        """Get a list of RepoData that are used to as inputs to the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with readable repositories, in the order to be used when searching.
        """
        if self._inputs is None:
            raise RuntimeError("Inputs not yet initialized.")
        return self._inputs

    def outputs(self):
        """Get a list of RepoData that are used to as outputs to the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with writable repositories, in the order to be use when searching.
        """
        if self._outputs is None:
            raise RuntimeError("Outputs not yet initialized.")
        return self._outputs

    def all(self):
        """Get a list of all RepoData that are used to as by the Butler.
        The list is created lazily as needed, and cached.

        Returns
        -------
        A list of RepoData with writable repositories, in the order to be use when searching.
        """
        return self._all

    def __repr__(self):
        return "%s(_inputs=%r, \n_outputs=%s, \n_all=%s)" % (
            self.__class__.__name__,
            self._inputs,
            self._outputs,
            self._all)

    def _buildLookupLists(self):
        """Build the inputs and outputs lists based on the order of self.all()."""

        def addToList(repoData, lst):
            """Add a repoData and each of its parents (depth first) to a list"""
            if id(repoData) in alreadyAdded:
                return
            lst.append(repoData)
            alreadyAdded.add(id(repoData))
            for parent in repoData.parentRepoDatas:
                addToList(parent, lst)

        if self._inputs is not None or self._outputs is not None:
            raise RuntimeError("Lookup lists are already built.")
        inputs = [repoData for repoData in self.all() if repoData.role == 'input']
        outputs = [repoData for repoData in self.all() if repoData.role == 'output']
        self._inputs = []
        alreadyAdded = set()
        for repoData in outputs:
            if 'r' in repoData.repoArgs.mode:
                addToList(repoData.repoData, self._inputs)
        for repoData in inputs:
            addToList(repoData.repoData, self._inputs)
        self._outputs = [repoData.repoData for repoData in outputs]


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

    The preferred method of initialization is to use the `inputs` and `outputs` __init__ parameters. These
    are described in the parameters section, below.

    For backward compatibility: this initialization method signature can take a posix root path, and
    optionally a mapper class instance or class type that will be instantiated using the mapperArgs input
    argument. However, for this to work in a backward compatible way it creates a single repository that is
    used as both an input and an output repository. This is NOT preferred, and will likely break any
    provenance system we have in place.

    Parameters
    ----------
    root : string
        .. note:: Deprecated in 12_0
                  `root` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        A file system path. Will only work with a PosixRepository.
    mapper : string or instance
        .. note:: Deprecated in 12_0
                  `mapper` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        Provides a mapper to be used with Butler.
    mapperArgs : dict
        .. note:: Deprecated in 12_0
                  `mapperArgs` will be removed in TBD, it is replaced by `inputs` and `outputs` for
                  multiple-repository support.
        Provides arguments to be passed to the mapper if the mapper input argument is a class type to be
        instantiated by Butler.
    inputs : RepositoryArgs, dict, or string
        Can be a single item or a list. Provides arguments to load an existing repository (or repositories).
        String is assumed to be a URI and is used as the cfgRoot (URI to the location of the cfg file). (Local
        file system URI does not have to start with 'file://' and in this way can be a relative path). The
        `RepositoryArgs` class can be used to provide more parameters with which to initialize a repository
        (such as `mapper`, `mapperArgs`, `tags`, etc. See the `RepositoryArgs` documentation for more
        details). A dict may be used as shorthand for a `RepositoryArgs` class instance. The dict keys must
        match parameters to the `RepositoryArgs.__init__` function.
    outputs : RepositoryArgs, dict, or string
        Provides arguments to load one or more existing repositories or create new ones. The different types
        are handled the same as for `inputs`.

    The Butler init sequence loads all of the input and output repositories.
    This creates the object hierarchy to read from and write to them. Each
    repository can have 0 or more parents, which also get loaded as inputs.
    This becomes a DAG of repositories. Ultimately, Butler creates a list of
    these Repositories in the order that they are used.

    Initialization Sequence
    =======================

    During initialization Butler creates a Repository class instance & support structure for each object
    passed to `inputs` and `outputs` as well as the parent repositories recorded in the `RepositoryCfg` of
    each existing readable repository.

    This process is complex. It is explained below to shed some light on the intent of each step.

    1. Input Argument Standardization
    ---------------------------------

    In `Butler._processInputArguments` the input arguments are verified to be legal (and a RuntimeError is
    raised if not), and they are converted into an expected format that is used for the rest of the Butler
    init sequence. See the docstring for `_processInputArguments`.

    2. Create RepoData Objects
    --------------------------

    Butler uses an object, called `RepoData`, to keep track of information about each repository; each
    repository is contained in a single `RepoData`. The attributes are explained in its docstring.

    After `_processInputArguments`, a RepoData is instantiated and put in a list for each repository in
    `outputs` and `inputs`. This list of RepoData, the `repoDataList`, now represents all the output and input
    repositories (but not parent repositories) that this Butler instance will use.

    3. Get `RepositoryCfg`s
    -----------------------

    `Butler._getCfgs` gets the `RepositoryCfg` for each repository the `repoDataList`. The behavior is
    described in the docstring.

    4. Add Parents
    --------------

    `Butler._addParents` then considers the parents list in the `RepositoryCfg` of each `RepoData` in the
    `repoDataList` and inserts new `RepoData` objects for each parent not represented in the proper location
    in the `repoDataList`. Ultimately a flat list is built to represent the DAG of readable repositories
    represented in depth-first order.

    5. Set and Verify Parents of Outputs
    ------------------------------------

    To be able to load parent repositories when output repositories are used as inputs, the input repositories
    are recorded as parents in the `RepositoryCfg` file of new output repositories. When an output repository
    already exists, for consistency the Butler's inputs must match the list of parents specified the already-
    existing output repository's `RepositoryCfg` file.

    In `Butler._setAndVerifyParentsLists`, the list of parents is recorded in the `RepositoryCfg` of new
    repositories. For existing repositories the list of parents is compared with the `RepositoryCfg`'s parents
    list, and if they do not match a `RuntimeError` is raised.

    6. Set the Default Mapper
    -------------------------

    If all the input repositories use the same mapper then we can assume that mapper to be the
    "default mapper". If there are new output repositories whose `RepositoryArgs` do not specify a mapper and
    there is a default mapper then the new output repository will be set to use that default mapper.

    This is handled in `Butler._setDefaultMapper`.

    7. Cache References to Parent RepoDatas
    ---------------------------------------

    In `Butler._connectParentRepoDatas`, in each `RepoData` in `repoDataList`, a list of `RepoData` object
    references is  built that matches the parents specified in that `RepoData`'s `RepositoryCfg`.

    This list is used later to find things in that repository's parents, without considering peer repository's
    parents. (e.g. finding the registry of a parent)

    8. Set Tags
    -----------

    Tags are described at https://ldm-463.lsst.io/v/draft/#tagging

    In `Butler._setRepoDataTags`, for each `RepoData`, the tags specified by its `RepositoryArgs` are recorded
    in a set, and added to the tags set in each of its parents, for ease of lookup when mapping.

    9. Find Parent Registry and Instantiate RepoData
    ------------------------------------------------

    At this point there is enough information to instantiate the `Repository` instances. There is one final
    step before instantiating the Repository, which is to try to get a parent registry that can be used by the
    child repository. The criteria for "can be used" is spelled out in `Butler._setParentRegistry`. However,
    to get the registry from the parent, the parent must be instantiated. The `repoDataList`, in depth-first
    search order, is built so that the most-dependent repositories are first, and the least dependent
    repositories are last. So the `repoDataList` is reversed and the Repositories are instantiated in that
    order; for each RepoData a parent registry is searched for, and then the Repository is instantiated with
    whatever registry could be found."""

    GENERATION = 2
    """This is a Generation 2 Butler.
    """

    def __init__(self, root=None, mapper=None, inputs=None, outputs=None, **mapperArgs):
        self._initArgs = {'root': root, 'mapper': mapper, 'inputs': inputs, 'outputs': outputs,
                          'mapperArgs': mapperArgs}

        self.log = Log.getLogger("daf.persistence.butler")

        inputs, outputs = self._processInputArguments(
            root=root, mapper=mapper, inputs=inputs, outputs=outputs, **mapperArgs)

        # convert the RepoArgs into RepoData
        inputs = [RepoData(args, 'input') for args in inputs]
        outputs = [RepoData(args, 'output') for args in outputs]
        repoDataList = outputs + inputs

        self._getCfgs(repoDataList)

        self._addParents(repoDataList)

        self._setAndVerifyParentsLists(repoDataList)

        self._setDefaultMapper(repoDataList)

        self._connectParentRepoDatas(repoDataList)

        self._repos = RepoDataContainer(repoDataList)

        self._setRepoDataTags()

        for repoData in repoDataList:
            self._initRepo(repoData)

    def _initRepo(self, repoData):
        if repoData.repo is not None:
            # this repository may have already been initialized by its children, in which case there is
            # nothing more to do.
            return
        for parentRepoData in repoData.parentRepoDatas:
            if parentRepoData.cfg.mapper != repoData.cfg.mapper:
                continue
            if parentRepoData.repo is None:
                self._initRepo(parentRepoData)
            parentRegistry = parentRepoData.repo.getRegistry()
            repoData.parentRegistry = parentRegistry if parentRegistry else parentRepoData.parentRegistry
            if repoData.parentRegistry:
                break
        repoData.repo = Repository(repoData)

    def _processInputArguments(self, root=None, mapper=None, inputs=None, outputs=None, **mapperArgs):
        """Process, verify, and standardize the input arguments.
        * Inputs can not be for Old Butler (root, mapper, mapperArgs) AND New Butler (inputs, outputs)
          `root`, `mapper`, and `mapperArgs` are Old Butler init API.
          `inputs` and `outputs` are New Butler init API.
           Old Butler and New Butler init API may not be mixed, Butler may be initialized with only the Old
           arguments or the New arguments.
        * Verify that if there is a readable output that there is exactly one output. (This restriction is in
          place because all readable repositories must be parents of writable   repositories, and for
          consistency the DAG of readable repositories must always be the same. Keeping the list   of parents
          becomes very complicated in the presence of multiple readable output repositories. It is better to
          only write to output repositories, and then create a new Butler instance and use the outputs as
          inputs, and write to new output repositories.)
        * Make a copy of inputs & outputs so they may be modified without changing the passed-in arguments.
        * Convert any input/output values that are URI strings to RepositoryArgs.
        * Listify inputs & outputs.
        * Set default RW mode on inputs & outputs as needed.

        Parameters
        ----------
        Same as Butler.__init__

        Returns
        -------
        (list of RepositoryArgs, list of RepositoryArgs)
            First item is a list to use as inputs.
            Second item is a list to use as outputs.

        Raises
        ------
        RuntimeError
            If Old Butler and New Butler arguments are both used this will raise.
            If an output is readable there is more than one output this will raise.
        """
        # inputs and outputs may be modified, do not change the external value.
        inputs = copy.deepcopy(inputs)
        outputs = copy.deepcopy(outputs)

        isV1Args = inputs is None and outputs is None
        if isV1Args:
            inputs, outputs = self._convertV1Args(root=root,
                                                  mapper=mapper,
                                                  mapperArgs=mapperArgs or None)
        elif root or mapper or mapperArgs:
            raise RuntimeError(
                'Butler version 1 API (root, mapper, **mapperArgs) may ' +
                'not be used with version 2 API (inputs, outputs)')
        self.datasetTypeAliasDict = {}

        self.storage = Storage()

        # make sure inputs and outputs are lists, and if list items are a string convert it RepositoryArgs.
        inputs = listify(inputs)
        outputs = listify(outputs)
        inputs = [RepositoryArgs(cfgRoot=args)
                  if not isinstance(args, RepositoryArgs) else args for args in inputs]
        outputs = [RepositoryArgs(cfgRoot=args)
                   if not isinstance(args, RepositoryArgs) else args for args in outputs]
        # Set the default value of inputs & outputs, verify the required values ('r' for inputs, 'w' for
        # outputs) and remove the 'w' from inputs if needed.
        for args in inputs:
            if args.mode is None:
                args.mode = 'r'
            elif 'rw' == args.mode:
                args.mode = 'r'
            elif 'r' != args.mode:
                raise RuntimeError("The mode of an input should be readable.")
        for args in outputs:
            if args.mode is None:
                args.mode = 'w'
            elif 'w' not in args.mode:
                raise RuntimeError("The mode of an output should be writable.")
        # check for class instances in args.mapper (not allowed)
        for args in inputs + outputs:
            if (args.mapper and not isinstance(args.mapper, basestring) and
               not inspect.isclass(args.mapper)):
                self.log.warn(preinitedMapperWarning)
        # if the output is readable, there must be only one output:
        for o in outputs:
            if 'r' in o.mode:
                if len(outputs) > 1:
                    raise RuntimeError("Butler does not support multiple output repositories if any of the "
                                       "outputs are readable.")

        # Handle the case where the output is readable and is also passed in as one of the inputs by removing
        # the input. This supports a legacy use case in pipe_tasks where the input is also passed as the
        # output, to the command line parser.
        def inputIsInOutputs(inputArgs, outputArgsList):
            for o in outputArgsList:
                if ('r' in o.mode and
                        o.root == inputArgs.root and
                        o.mapper == inputArgs.mapper and
                        o.mapperArgs == inputArgs.mapperArgs and
                        o.tags == inputArgs.tags and
                        o.policy == inputArgs.policy):
                    self.log.debug(("Input repositoryArgs {} is also listed in outputs as readable; " +
                                    "throwing away the input.").format(inputArgs))
                    return True
            return False

        inputs = [args for args in inputs if not inputIsInOutputs(args, outputs)]
        return inputs, outputs

    @staticmethod
    def _getParentVal(repoData):
        """Get the value of this repoData as it should appear in the parents
        list of other repositories"""
        if repoData.isV1Repository:
            return repoData.cfg
        if repoData.cfgOrigin == 'nested':
            return repoData.cfg
        else:
            return repoData.cfg.root

    @staticmethod
    def _getParents(ofRepoData, repoInfo):
        """Create a parents list of repoData from inputs and (readable) outputs."""
        parents = []
        # get the parents list of repoData:
        for repoData in repoInfo:
            if repoData is ofRepoData:
                continue
            if 'r' not in repoData.repoArgs.mode:
                continue
            parents.append(Butler._getParentVal(repoData))
        return parents

    @staticmethod
    def _getOldButlerRepositoryCfg(repositoryArgs):
        if not Storage.isPosix(repositoryArgs.cfgRoot):
            return None
        if not PosixStorage.v1RepoExists(repositoryArgs.cfgRoot):
            return None
        if not repositoryArgs.mapper:
            repositoryArgs.mapper = PosixStorage.getMapperClass(repositoryArgs.cfgRoot)
        cfg = RepositoryCfg.makeFromArgs(repositoryArgs)
        parent = PosixStorage.getParentSymlinkPath(repositoryArgs.cfgRoot)
        if parent:
            parent = Butler._getOldButlerRepositoryCfg(RepositoryArgs(cfgRoot=parent, mode='r'))
            if parent is not None:
                cfg.addParents([parent])
        return cfg

    def _getRepositoryCfg(self, repositoryArgs):
        """Try to get a repository from the location described by cfgRoot.

        Parameters
        ----------
        repositoryArgs : RepositoryArgs or string
            Provides arguments to load an existing repository (or repositories). String is assumed to be a URI
            and is used as the cfgRoot (URI to the location of the cfg file).

        Returned
        --------
        (RepositoryCfg or None, bool)
            The RepositoryCfg, or None if one cannot be found, and True if the RepositoryCfg was created by
            reading an Old Butler repository, or False if it is a New Butler Repository.
        """
        if not isinstance(repositoryArgs, RepositoryArgs):
            repositoryArgs = RepositoryArgs(cfgRoot=repositoryArgs, mode='r')

        cfg = self.storage.getRepositoryCfg(repositoryArgs.cfgRoot)
        isOldButlerRepository = False
        if cfg is None:
            cfg = Butler._getOldButlerRepositoryCfg(repositoryArgs)
            if cfg is not None:
                isOldButlerRepository = True
        return cfg, isOldButlerRepository

    def _getCfgs(self, repoDataList):
        """Get or make a RepositoryCfg for each RepoData, and add the cfg to the RepoData.
        If the cfg exists, compare values. If values match then use the cfg as an "existing" cfg. If the
        values do not match, use the cfg as a "nested" cfg.
        If the cfg does not exist, the RepositoryArgs must be for a writable repository.

        Parameters
        ----------
        repoDataList : list of RepoData
            The RepoData that are output and inputs of this Butler

        Raises
        ------
        RuntimeError
            If the passed-in RepositoryArgs indicate an existing repository but other cfg parameters in those
            RepositoryArgs don't
            match the existing repository's cfg a RuntimeError will be raised.
        """
        def cfgMatchesArgs(args, cfg):
            """Test if there are any values in an RepositoryArgs that conflict with the values in a cfg"""
            if args.mapper is not None and cfg.mapper != args.mapper:
                return False
            if args.mapperArgs is not None and cfg.mapperArgs != args.mapperArgs:
                return False
            if args.policy is not None and cfg.policy != args.policy:
                return False
            return True

        for repoData in repoDataList:
            cfg, isOldButlerRepository = self._getRepositoryCfg(repoData.repoArgs)
            if cfg is None:
                if 'w' not in repoData.repoArgs.mode:
                    raise RuntimeError(
                        "No cfg found for read-only input repository at {}".format(repoData.repoArgs.cfgRoot))
                repoData.setCfg(cfg=RepositoryCfg.makeFromArgs(repoData.repoArgs),
                                origin='new',
                                root=repoData.repoArgs.cfgRoot,
                                isV1Repository=isOldButlerRepository)
            else:

                # This is a hack fix for an issue introduced by DM-11284; Old Butler parent repositories used
                # to be stored as a path to the repository in the parents list and it was changed so that the
                # whole RepositoryCfg, that described the Old Butler repository (including the mapperArgs that
                # were used with it), was recorded as a "nested" repository cfg. That checkin did not account
                # for the fact that there were repositoryCfg.yaml files in the world with only the path to
                # Old Butler repositories in the parents list.
                if cfg.parents:
                    for i, parent in enumerate(cfg.parents):
                        if isinstance(parent, RepositoryCfg):
                            continue
                        parentCfg, parentIsOldButlerRepository = self._getRepositoryCfg(parent)
                        if parentIsOldButlerRepository:
                            parentCfg.mapperArgs = cfg.mapperArgs
                            self.log.info(("Butler is replacing an Old Butler parent repository path '{}' "
                                           "found in the parents list of a New Butler repositoryCfg: {} "
                                           "with a repositoryCfg that includes the child repository's "
                                           "mapperArgs: {}. This affects the instantiated RepositoryCfg "
                                           "but does not change the persisted child repositoryCfg.yaml file."
                                           ).format(parent, cfg, parentCfg))
                            cfg._parents[i] = cfg._normalizeParents(cfg.root, [parentCfg])[0]

                if 'w' in repoData.repoArgs.mode:
                    # if it's an output repository, the RepositoryArgs must match the existing cfg.
                    if not cfgMatchesArgs(repoData.repoArgs, cfg):
                        raise RuntimeError(("The RepositoryArgs and RepositoryCfg must match for writable " +
                                            "repositories, RepositoryCfg:{}, RepositoryArgs:{}").format(
                                                cfg, repoData.repoArgs))
                    repoData.setCfg(cfg=cfg, origin='existing', root=repoData.repoArgs.cfgRoot,
                                    isV1Repository=isOldButlerRepository)
                else:
                    # if it's an input repository, the cfg can overwrite the in-repo cfg.
                    if cfgMatchesArgs(repoData.repoArgs, cfg):
                        repoData.setCfg(cfg=cfg, origin='existing', root=repoData.repoArgs.cfgRoot,
                                        isV1Repository=isOldButlerRepository)
                    else:
                        repoData.setCfg(cfg=cfg, origin='nested', root=None,
                                        isV1Repository=isOldButlerRepository)

    def _addParents(self, repoDataList):
        """For each repoData in the input list, see if its parents are the next items in the list, and if not
        add the parent, so that the repoDataList includes parents and is in order to operate depth-first 0..n.

        Parameters
        ----------
        repoDataList : list of RepoData
            The RepoData for the Butler outputs + inputs.

        Raises
        ------
        RuntimeError
            Raised if a RepositoryCfg can not be found at a location where a parent repository should be.
        """
        repoDataIdx = 0
        while True:
            if repoDataIdx == len(repoDataList):
                break
            repoData = repoDataList[repoDataIdx]
            if 'r' not in repoData.repoArgs.mode:
                repoDataIdx += 1
                continue  # the repoData only needs parents if it's readable.
            if repoData.isNewRepository:
                repoDataIdx += 1
                continue  # if it's new the parents will be the inputs of this butler.
            if repoData.cfg.parents is None:
                repoDataIdx += 1
                continue  # if there are no parents then there's nothing to do.
            for repoParentIdx, repoParent in enumerate(repoData.cfg.parents):
                parentIdxInRepoDataList = repoDataIdx + repoParentIdx + 1
                if not isinstance(repoParent, RepositoryCfg):
                    repoParentCfg, isOldButlerRepository = self._getRepositoryCfg(repoParent)
                    if repoParentCfg is not None:
                        cfgOrigin = 'existing'
                else:
                    isOldButlerRepository = False
                    repoParentCfg = repoParent
                    cfgOrigin = 'nested'
                if (parentIdxInRepoDataList < len(repoDataList) and
                        repoDataList[parentIdxInRepoDataList].cfg == repoParentCfg):
                    continue
                args = RepositoryArgs(cfgRoot=repoParentCfg.root, mode='r')
                role = 'input' if repoData.role == 'output' else 'parent'
                newRepoInfo = RepoData(args, role)
                newRepoInfo.repoData.setCfg(cfg=repoParentCfg, origin=cfgOrigin, root=args.cfgRoot,
                                            isV1Repository=isOldButlerRepository)
                repoDataList.insert(parentIdxInRepoDataList, newRepoInfo)
            repoDataIdx += 1

    def _setAndVerifyParentsLists(self, repoDataList):
        """Make a list of all the input repositories of this Butler, these are the parents of the outputs.
        For new output repositories, set the parents in the RepositoryCfg. For existing output repositories
        verify that the RepositoryCfg's parents match the parents list.

        Parameters
        ----------
        repoDataList : list of RepoData
            All the RepoDatas loaded by this butler, in search order.

        Raises
        ------
        RuntimeError
            If an existing output repository is loaded and its parents do not match the parents of this Butler
            an error will be raised.
        """
        def getIOParents(ofRepoData, repoDataList):
            """make a parents list for repo in `ofRepoData` that is comprised of inputs and readable
            outputs (not parents-of-parents) of this butler"""
            parents = []
            for repoData in repoDataList:
                if repoData.role == 'parent':
                    continue
                if repoData is ofRepoData:
                    continue
                if repoData.role == 'output':
                    if 'r' in repoData.repoArgs.mode:
                        raise RuntimeError("If an output is readable it must be the only output.")
                        # and if this is the only output, this should have continued in
                        # "if repoData is ofRepoData"
                    continue
                parents.append(self._getParentVal(repoData))
            return parents

        for repoData in repoDataList:
            if repoData.role != 'output':
                continue
            parents = getIOParents(repoData, repoDataList)
            # if repoData is new, add the parent RepositoryCfgs to it.
            if repoData.cfgOrigin == 'new':
                repoData.cfg.addParents(parents)
            elif repoData.cfgOrigin in ('existing', 'nested'):
                if repoData.cfg.parents != parents:
                    try:
                        repoData.cfg.extendParents(parents)
                    except ParentsMismatch as e:
                        raise RuntimeError(("Inputs of this Butler:{} do not match parents of existing " +
                                           "writable cfg:{} (ParentMismatch exception: {}").format(
                                           parents, repoData.cfg.parents, e))

    def _setDefaultMapper(self, repoDataList):
        """Establish a default mapper if there is one and assign it to outputs that do not have a mapper
        assigned.

        If all inputs have the same mapper it will be used as the default mapper.

        Parameters
        ----------
        repoDataList : list of RepoData
            All the RepoDatas loaded by this butler, in search order.

        Raises
        ------
        RuntimeError
            If a default mapper can not be established and there is an output that does not have a mapper.
        """
        needyOutputs = [rd for rd in repoDataList if rd.role == 'output' and rd.cfg.mapper is None]
        if len(needyOutputs) == 0:
            return
        mappers = set([rd.cfg.mapper for rd in repoDataList if rd.role == 'input'])
        if len(mappers) != 1:
            inputs = [rd for rd in repoDataList if rd.role == 'input']
            raise RuntimeError(
                ("No default mapper could be established from inputs:{} and no mapper specified " +
                 "for outputs:{}").format(inputs, needyOutputs))
        defaultMapper = mappers.pop()
        for repoData in needyOutputs:
            repoData.cfg.mapper = defaultMapper

    def _connectParentRepoDatas(self, repoDataList):
        """For each RepoData in repoDataList, find its parent in the repoDataList and cache a reference to it.

        Parameters
        ----------
        repoDataList : list of RepoData
            All the RepoDatas loaded by this butler, in search order.

        Raises
        ------
        RuntimeError
            When a parent is listed in the parents list but not found in the repoDataList. This is not
            expected to ever happen and would indicate an internal Butler error.
        """
        for repoData in repoDataList:
            for parent in repoData.cfg.parents:
                parentToAdd = None
                for otherRepoData in repoDataList:
                    if isinstance(parent, RepositoryCfg):
                        if otherRepoData.repoData.repoData.cfg == parent:
                            parentToAdd = otherRepoData.repoData
                            break
                    elif otherRepoData.repoData.cfg.root == parent:
                        parentToAdd = otherRepoData.repoData
                        break
                if parentToAdd is None:
                    raise RuntimeError(
                        "Could not find a parent matching {} to add to {}".format(parent, repoData))
                repoData.addParentRepoData(parentToAdd)

    @staticmethod
    def _getParentRepoData(parent, repoDataList):
        """get a parent RepoData from a cfg from a list of RepoData

        Parameters
        ----------
        parent : string or RepositoryCfg
            cfgRoot of a repo or a cfg that describes the repo
        repoDataList : list of RepoData
            list to search in

        Returns
        -------
        RepoData or None
            A RepoData if one can be found, else None
        """
        repoData = None
        for otherRepoData in repoDataList:
            if isinstance(parent, RepositoryCfg):
                if otherRepoData.cfg == parent:
                    repoData = otherRepoData
                    break
            elif otherRepoData.cfg.root == parent:
                repoData = otherRepoData
                break
        return repoData

    def _setRepoDataTags(self):
        """Set the tags from each repoArgs into all its parent repoArgs so that they can be included in tagged
        searches."""
        def setTags(repoData, tags, context):
            if id(repoData) in context:
                return
            repoData.addTags(tags)
            context.add(id(repoData))
            for parentRepoData in repoData.parentRepoDatas:
                setTags(parentRepoData, tags, context)
        for repoData in self._repos.outputs() + self._repos.inputs():
            setTags(repoData.repoData, repoData.repoArgs.tags, set())

    def _convertV1Args(self, root, mapper, mapperArgs):
        """Convert Old Butler RepositoryArgs (root, mapper, mapperArgs) to New Butler RepositoryArgs
        (inputs, outputs)

        Parameters
        ----------
        root : string
            Posix path to repository root
        mapper : class, class instance, or string
            Instantiated class, a class object to be instantiated, or a string that refers to a class that
            can be imported & used as the mapper.
        mapperArgs : dict
            RepositoryArgs & their values used when instantiating the mapper.

        Returns
        -------
        tuple
            (inputs, outputs) - values to be used for inputs and outputs in Butler.__init__
        """
        if (mapper and not isinstance(mapper, basestring) and
           not inspect.isclass(mapper)):
            self.log.warn(preinitedMapperWarning)
        inputs = None
        if root is None:
            if hasattr(mapper, 'root'):
                # in legacy repositories, the mapper may be given the root directly.
                root = mapper.root
            else:
                # in the past root="None" could be used to mean root='.'
                root = '.'
        outputs = RepositoryArgs(mode='rw',
                                 root=root,
                                 mapper=mapper,
                                 mapperArgs=mapperArgs)
        return inputs, outputs

    def __repr__(self):
        return 'Butler(datasetTypeAliasDict=%s, repos=%s)' % (
            self.datasetTypeAliasDict, self._repos)

    def _getDefaultMapper(self):

        """Get the default mapper. Currently this means if all the repositories use exactly the same mapper,
        that mapper may be considered the default.

        This definition may be changing; mappers may be able to exclude themselves as candidates for default,
        and they may nominate a different mapper instead. Also, we may not want to look at *all* the
        repositories, but only a depth-first search on each of the input & output repositories, and use the
        first-found mapper for each of those. TBD.

        Parameters
        ----------
        inputs : TYPE
            Description

        Returns
        -------
        Mapper class or None
            Returns the class type of the default mapper, or None if a default
            mapper can not be determined.
        """
        defaultMapper = None

        for inputRepoData in self._repos.inputs():
            mapper = None
            if inputRepoData.cfg.mapper is not None:
                mapper = inputRepoData.cfg.mapper
                # if the mapper is:
                # * a string, import it.
                # * a class instance, get its class type
                # * a class, do nothing; use it
                if isinstance(mapper, basestring):
                    mapper = doImport(mapper)
                elif not inspect.isclass(mapper):
                    mapper = mapper.__class__
            # If no mapper has been found, note the first found mapper.
            # Then, if a mapper has been found and each next mapper matches it,
            # continue looking for mappers.
            # If a mapper has been found and another non-matching mapper is
            # found then we have no default, return None.
            if defaultMapper is None:
                defaultMapper = mapper
            elif mapper == defaultMapper:
                continue
            elif mapper is not None:
                return None
        return defaultMapper

    def _assignDefaultMapper(self, defaultMapper):
        for repoData in self._repos.all().values():
            if repoData.cfg.mapper is None and (repoData.isNewRepository or repoData.isV1Repository):
                if defaultMapper is None:
                    raise RuntimeError(
                        "No mapper specified for %s and no default mapper could be determined." %
                        repoData.args)
                repoData.cfg.mapper = defaultMapper

    @staticmethod
    def getMapperClass(root):
        """posix-only; gets the mapper class at the path specified by root (if a file _mapper can be found at
        that location or in a parent location.

        As we abstract the storage and support different types of storage locations this method will be
        moved entirely into Butler Access, or made more dynamic, and the API will very likely change."""
        return Storage.getMapperClass(root)

    def defineAlias(self, alias, datasetType):
        """Register an alias that will be substituted in datasetTypes.

        Parameters
        ----------
        alias - string
            The alias keyword. It may start with @ or not. It may not contain @ except as the first character.
        datasetType - string
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
        float, string).

        Parameters
        ----------
        datasetType - string
            The type of dataset to get keys for, entire collection if None.
        level - string
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
        corresponding to the keys (int, float, string).
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

    def queryMetadata(self, datasetType, format, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.

        Parameters
        ----------
        datasetType - string
            The type of dataset to inquire about.
        format - str, tuple
            Key or tuple of keys to be returned.
        dataId - DataId, dict
            The partial data id.
        **rest -
            Keyword arguments for the partial data id.

        Returns
        -------
        A list of valid values or tuples of valid values as specified by the
        format.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)
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

    def datasetExists(self, datasetType, dataId={}, write=False, **rest):
        """Determines if a dataset file exists.

        Parameters
        ----------
        datasetType - string
            The type of dataset to inquire about.
        dataId - DataId, dict
            The data id of the dataset.
        write - bool
            If True, look only in locations where the dataset could be written,
            and return True only if it is present in all of them.
        **rest keyword arguments for the data id.

        Returns
        -------
        exists - bool
            True if the dataset exists or is non-file-based.
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)
        locations = self._locate(datasetType, dataId, write=write)
        if not write:  # when write=False, locations is not a sequence
            if locations is None:
                return False
            locations = [locations]

        if not locations:  # empty list
            return False

        for location in locations:
            # If the location is a ButlerComposite (as opposed to a ButlerLocation),
            # verify the component objects exist.
            if isinstance(location, ButlerComposite):
                for name, componentInfo in location.componentInfo.items():
                    if componentInfo.subset:
                        subset = self.subset(datasetType=componentInfo.datasetType, dataId=location.dataId)
                        exists = all([obj.datasetExists() for obj in subset])
                    else:
                        exists = self.datasetExists(componentInfo.datasetType, location.dataId)
                    if exists is False:
                        return False
            else:
                if not location.repository.exists(location):
                    return False
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
            try:
                location = repoData.repo.map(datasetType, dataId, write=write)
            except NoResults:
                continue
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
                # if a component location is not found, we can not continue with this repo, move to next repo.
                if location is None:
                    break
            # if reading, only one location is desired.
            if location:
                if not write:
                    # If there is a bypass function for this dataset type, we can't test to see if the object
                    # exists in storage, because the bypass function may not actually use the location
                    # according to the template. Instead, execute the bypass function and include its results
                    # in the bypass attribute of the location. The bypass function may fail for any reason,
                    # the most common case being that a file does not exist. If it raises an exception
                    # indicating such, we ignore the bypass function and proceed as though it does not exist.
                    if hasattr(location.mapper, "bypass_" + location.datasetType):
                        bypass = self._getBypassFunc(location, dataId)
                        try:
                            bypass = bypass()
                            location.bypass = bypass
                        except (NoResults, IOError):
                            self.log.debug("Continuing dataset search while evaluating "
                                           "bypass function for Dataset type:{} Data ID:{} at "
                                           "location {}".format(datasetType, dataId, location))
                    # If a location was found but the location does not exist, keep looking in input
                    # repositories (the registry may have had enough data for a lookup even thought the object
                    # exists in a different repository.)
                    if (isinstance(location, ButlerComposite) or hasattr(location, 'bypass') or
                            location.repository.exists(location)):
                        return location
                else:
                    try:
                        locations.extend(location)
                    except TypeError:
                        locations.append(location)
        if not write:
            return None
        return locations

    @staticmethod
    def _getBypassFunc(location, dataId):
        pythonType = location.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                pythonType = doImport(pythonType)
        bypassFunc = getattr(location.mapper, "bypass_" + location.datasetType)
        return lambda: bypassFunc(location.datasetType, pythonType, location, dataId)

    def get(self, datasetType, dataId=None, immediate=True, **rest):
        """Retrieves a dataset given an input collection data id.

        Parameters
        ----------
        datasetType - string
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

        if hasattr(location, 'bypass'):
            # this type loader block should get moved into a helper someplace, and duplications removed.
            def callback():
                return location.bypass
        else:
            def callback():
                return self._read(location)
        if location.mapper.canStandardize(location.datasetType):
            innerCallback = callback

            def callback():
                return location.mapper.standardize(location.datasetType, innerCallback(), dataId)
        if immediate:
            return callback()
        return ReadProxy(callback)

    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        """Persists a dataset given an output collection data id.

        Parameters
        ----------
        obj -
            The object to persist.
        datasetType - string
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

        locations = self._locate(datasetType, dataId, write=True)
        if not locations:
            raise NoResults("No locations for put:", datasetType, dataId)
        for location in locations:
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
        datasetType - string
            The type of dataset collection to subset
        level - string
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
        datasetType - string
            The type of dataset collection to reference
        level - string
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

    def getUri(self, datasetType, dataId=None, write=False, **rest):
        """Return the URI for a dataset

        .. warning:: This is intended only for debugging. The URI should
        never be used for anything other than printing.

        .. note:: In the event there are multiple URIs for read, we return only
        the first.

        .. note:: getUri() does not currently support composite datasets.

        Parameters
        ----------
        datasetType : `str`
           The dataset type of interest.
        dataId : `dict`, optional
           The data identifier.
        write : `bool`, optional
           Return the URI for writing?
        rest : `dict`, optional
           Keyword arguments for the data id.

        Returns
        -------
        uri : `str`
           URI for dataset.
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = DataId(dataId)
        dataId.update(**rest)
        locations = self._locate(datasetType, dataId, write=write)
        if locations is None:
            raise NoResults("No locations for getUri: ", datasetType, dataId)

        if write:
            # Follow the write path
            # Return the first valid write location.
            for location in locations:
                if isinstance(location, ButlerComposite):
                    for name, info in location.componentInfo.items():
                        if not info.inputOnly:
                            return self.getUri(info.datasetType, location.dataId, write=True)
                else:
                    return location.getLocationsWithRoot()[0]
            # fall back to raise
            raise NoResults("No locations for getUri(write=True): ", datasetType, dataId)
        else:
            # Follow the read path, only return the first valid read
            return locations.getLocationsWithRoot()[0]

    def _read(self, location):
        """Unpersist an object using data inside a ButlerLocation or ButlerComposite object.

        Parameters
        ----------
        location : ButlerLocation or ButlerComposite
            A ButlerLocation or ButlerComposite instance populated with data needed to read the object.

        Returns
        -------
        object
            An instance of the object specified by the location.
        """
        self.log.debug("Starting read from %s", location)

        if isinstance(location, ButlerComposite):
            for name, componentInfo in location.componentInfo.items():
                if componentInfo.subset:
                    subset = self.subset(datasetType=componentInfo.datasetType, dataId=location.dataId)
                    componentInfo.obj = [obj.get() for obj in subset]
                else:
                    obj = self.get(componentInfo.datasetType, location.dataId, immediate=True)
                    componentInfo.obj = obj
                assembler = location.assembler or genericAssembler
            results = assembler(dataId=location.dataId, componentInfo=location.componentInfo,
                                cls=location.python)
            return results
        else:
            results = location.repository.read(location)
            if len(results) == 1:
                results = results[0]
        self.log.debug("Ending read from %s", location)
        return results

    def __reduce__(self):
        ret = (_unreduce, (self._initArgs, self.datasetTypeAliasDict))
        return ret

    def _resolveDatasetTypeAlias(self, datasetType):
        """Replaces all the known alias keywords in the given string with the alias value.

        Parameters
        ----------
        datasetType - string
            A datasetType string to search & replace on

        Returns
        -------
        datasetType - string
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
