#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2015 LSST Corporation.
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

import collections
import copy
import os
import UserDict
import warnings
import yaml

from yaml.representer import Representer
yaml.add_representer(collections.defaultdict, Representer.represent_dict)

import lsst.pex.policy as pexPolicy
import lsst.utils

class Policy(UserDict.UserDict, yaml.YAMLObject):
    """Policy implements a datatype that is used by Butler for configuration parameters.
    It is essentially a dict with key/value pairs, including nested dicts (as values). In fact, it can be
    initialized with a dict. The only caveat is that keys may NOT contain dots ('.'). This is explained next:
    Policy extends the dict api so that hierarchical values may be accessed with dot-delimited notiation.
    That is, foo.getValue('a.b.c') is the same as foo['a']['b']['c'] is the same as foo['a.b.c'], and either
    of these syntaxes may be used.

    Storage formats supported:
    - yaml: read and write is supported.
    - pex policy: read is supported, although this is deprecated and will at some point be removed.
    """

    def __init__(self, other=None, preference=('data', 'policy', 'pexPolicy', 'filePath', 'defaultInitData'),
                 **kwargs):
        """Constructor. Accepts an optional vars, one of which will be used to initalize the Policy.
        If more than initializer var is populated, one will be selected in order specified by 'preference'.

        :param preference: a tuple in order of preference to be used if more than 1 initialzer var is
                           populated.
        :param kwargs:
            data: a dict with initial key-value pairs.
            policy: an instance of a Policy that should be copied into this Policy.
            filePath: path to a policy file. May be .yaml or .paf
            pexPolicy: an instance of a pex policy to copy into this Policy
            defaultInitData: policy data with a populated instance of a defaultInitData
                             e.g. Policy.defaultInitData('daf_persistence', 'testPolicy.yaml', 'tests')

        :return:
        """
        UserDict.UserDict.__init__(self)
        self.__init(other, preference, **kwargs)

    def ppprint(self):
        """helper function for debugging, prints a policy out in a readable way in the debugger.

        use: pdb> print myPolicyObject.pprint()
        :return: a prettyprint formatted string representing the policy
        """
        import pprint
        return pprint.pformat(self.data, indent=2, width=1)

    def __repr__(self):
        return self.data.__repr__()

    def __init(self, other, preference, **kwargs):
        for p in preference:
            if p in kwargs:
                if p == 'policy':
                    self.data = {}
                    self.merge(kwargs['policy'])
                    return
                if p == 'filePath':
                    self.__initFromFile(kwargs['filePath'])
                    return
                if p == 'pexPolicy':
                    self.__initFromPexPolicy(kwargs['pexPolicy'])
                    return
                if p == 'defaultInitData':
                    self.__initFromDefaultPolicy(preference, *kwargs['defaultInitData'])
                    return
                if p == 'data':
                    # don't assign other to self.data directly; need assign via parse in self.update.
                    self.update(kwargs['data'])
                    return

        if other is not None:
            self.data = {}
            self.update(other)

    def __initFromFile(self, paths):
        """Load a file from path. If path is a list, will pick one to use, according to order specified
        by extensionPreference.

        :param path: string or list of strings, to a persisted policy file.
        :param extensionPreference: the order in which to try to open files. Will use the first one that
        succeeds.
        :return:
        """
        extensionPreference = ('yaml', 'paf')
        matchedPath = None
        if isinstance(paths, basestring):
            paths = [paths]
        for ext in extensionPreference:
            if matchedPath is not None:
                break
            for path in paths:
                if path.endswith(ext):
                    matchedPath = path
                    break
        policy = None
        if matchedPath is None:
            raise IOError("Could not find policy at %s" %paths)
        elif matchedPath.endswith('yaml'):
            self.__initFromYamlFile(matchedPath)
        elif matchedPath.endswith('paf'):
            policy = pexPolicy.Policy.createPolicy(matchedPath)
            self.__initFromPexPolicy(policy)
        else:
            raise RuntimeError("Unhandled policy file type:%s" %matchedPath)

    def __initFromDefaultPolicy(self, preference, productName, filePath, repos):
        """Open a policy file by package name + location. Usually used for package-default policies.

        :param productName: the name of the product that contains the policy.
        :param filePath: the relative pathname (including file name) to the policy file.
        :param repos: optional. The subdirectory within the product where policy files are stored. If empty,
        the product install dir + filePath will be used.
        :return:
        """
        policyPath = os.path.join(lsst.utils.getPackageDir(productName), repos, filePath)
        if not os.path.exists(policyPath):
            raise RuntimeError("No policy at path:%s" %policyPath)
        self.__init(None, preference=preference, filePath=policyPath)

    def __initFromPexPolicy(self, pexPolicy):
        """Load values from a pex policy.

        :param pexPolicy:
        :return:
        """
        names = pexPolicy.names()
        names.sort()
        for name in names:
            if pexPolicy.getValueType(name) == pexPolicy.POLICY:
                if name in self:
                    continue
                else:
                    self[name] = {}
            else:
                if pexPolicy.isArray(name):
                    self[name] = pexPolicy.getArray(name)
                else:
                    self[name] = pexPolicy.get(name)
        return self

    def __initFromYamlFile(self, path):
        """Opens a file at a given path and attempts to load it in from yaml.

        :param path:
        :return:
        """
        f = file(path, 'r')
        self.__initFromYaml(f)

    def __initFromYaml(self, stream):
        """Loads a YAML policy from any readable stream that contains one.

        :param stream:
        :return:
        """
        # will raise yaml.YAMLError if there is an error loading the file.
        self.data = yaml.load(stream)
        return self

    def __getitem__ (self, name):
        data = self.data
        for key in name.split('.'):
            if data is None:
                return None
            if key in data:
                data = data[key]
            else:
                return None
        if isinstance(data, collections.Mapping):
            data = Policy(data=data)
        return data

    def __setitem__(self, name, value):
        if isinstance(value, collections.Mapping):
            keys = name.split('.')
            d = {}
            cur = d
            for key in keys[0:-1]:
                cur[key] = {}
                cur = cur[key]
            cur[keys[-1]] = value
            self.update(d)
        data = self.data
        keys = name.split('.')
        for key in keys[0:-1]:
            data = data.setdefault(key, {})
        data[keys[-1]] = value

    def __contains__(self, key):
        d = self.data
        keys = key.split('.')
        for k in keys[0:-1]:
            if k in d:
                d = d[k]
            else:
                return False
        return keys[-1] in d

    def update(self, other):
        """Like dict.update, but will add or modify keys in nested dicts, instead of overwriting the nested
        dict entirely.

        For example, for the given code:
        foo = {'a': {'b': 1}}
        foo.update({'a': {'c': 2}})

        If foo is a dict, then after the update foo == {'a': {'c': 2}}
        But if foo is a Policy, then after the update foo == {'a': {'b': 1, 'c': 2}}
        """
        def doUpdate(d, u):
            for k, v in u.iteritems():
                if isinstance(d, collections.Mapping):
                    if isinstance(v, collections.Mapping):
                        r = doUpdate(d.get(k, {}), v)
                        d[k] = r
                    else:
                        d[k] = u[k]
                else:
                    d = {k: u[k]}
            return d
        doUpdate(self.data, other)

    def merge(self, other):
        """Like Policy.update, but will add keys & values from other that DO NOT EXIST in self. Keys and
        values that already exist in self will NOT be overwritten.

        :param other:
        :return:
        """
        otherCopy = copy.deepcopy(other)
        otherCopy.update(self)
        self.data = otherCopy.data

    def names(self, topLevelOnly=False):
        """Get the dot-delimited name of all the keys in the hierarchy.
        NOTE: this is different than the built-in method dict.keys, which will return only the first level
        keys.
        """
        if topLevelOnly:
            return self.keys()

        def getKeys(d, keys, base):
            for key in d.keys():
                val = d[key]
                levelKey = base + '.' + key if base is not None else key
                keys.append(levelKey)
                if isinstance(val, collections.Mapping):
                    getKeys(val, keys, levelKey)
        keys = []
        getKeys(self.data, keys, None)
        return keys

    def asArray(self, name):
        """Get a value as an array. May contain one or more elements.

        :param key:
        :return:
        """
        val = self.get(name)
        if isinstance(val, basestring):
            val = [val]
        elif not isinstance(val, collections.Container):
            val = [val]
        return val

    # Deprecated methods that mimic pex_policy api.
    # These are supported (for now), but callers should use the dict api.

    def getValue(self, name):
        """Get the value for a parameter name/key. See class notes about dot-delimited access.

        :param name:
        :return: the value for the given name.
        """
        warnings.warn_explicit("Deprecated. Use []", DeprecationWarning)
        return self[name]

    def setValue(self, name, value):
        """Set the value for a parameter name/key. See class notes about dot-delimited access.

        :param name:
        :return: None
        """
        warnings.warn("Deprecated. Use []", DeprecationWarning)
        self[name] = value

    def mergeDefaults(self, other):
        """For any keys in other that are not present in self, sets that key and its value into self.

        :param other: another Policy
        :return: None
        """
        warnings.warn("Deprecated. Use .merge()", DeprecationWarning)
        self.merge(other)

    def exists(self, key):
        """Query if a key exists in this Policy

        :param key:
        :return: True if the key exists, else false.
        """
        warnings.warn("Deprecated. Use 'key in object'", DeprecationWarning)
        return key in self

    def getString(self, key):
        """Get the string value of a key.

        :param key:
        :return: the value for key
        """
        warnings.warn("Deprecated. Use []", DeprecationWarning)
        return str(self[key])

    def getBool(self, key):
        """Get the value of a key.

        :param key:
        :return: the value for key
        """
        warnings.warn("Deprecated. Use []", DeprecationWarning)
        return bool(self[key])

    def getPolicy(self, key):
        """Get a subpolicy.

        :param key:
        :return:
        """
        warnings.warn("Deprecated. Use []", DeprecationWarning)
        return self[key]

    def getStringArray(self, key):
        """Get a value as an array. May contain one or more elements.

        :param key:
        :return:
        """
        warnings.warn("Deprecated. Use asArray()", DeprecationWarning)
        val = self.get(key)
        if isinstance(val, basestring):
            val = [val]
        elif not isinstance(val, collections.Container):
            val = [val]
        return val

    def __lt__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data < other

    def __le__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data <= other

    def __eq__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data == other

    def __ne__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data != other

    def __gt__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data > other

    def __ge__(self, other):
        if isinstance(other, Policy):
            other = other.data
        return self.data >= other

    #######
    # i/o #

    def dump(self, output):
        """Writes the policy to a yaml stream.

        :param stream:
        :return:
        """
        data = copy.copy(self.data)
        yaml.dump(data, output)

    def dumpToFile(self, path):
        """Writes the policy to a file.

        :param path:
        :return:
        """
        f = open(path, 'w')
        self.dump(f)
        f.close()
