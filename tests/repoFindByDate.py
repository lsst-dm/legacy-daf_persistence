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
from future import standard_library
standard_library.install_aliases()
from builtins import object

import pickle
import collections
import datetime
import os
import shutil
import unittest
import yaml

import lsst.utils.tests
import lsst.daf.persistence as dp

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class PosixPickleStringHanlder(object):

    @staticmethod
    def get(butlerLocation):
        if butlerLocation.storageName != "PickleStorage":
            raise TypeError("PosixStoragePickleMapper only supports PickleStorage")
        location = butlerLocation.getLocations()[0]  # should never be more than 1 location
        with open(location, 'rb') as f:
            ret = pickle.load(f)
        return ret

    @staticmethod
    def put(obj, butlerLocation):
        if butlerLocation.storageName != "PickleStorage":
            raise TypeError("PosixStoragePickleMapper only supports PickleStorage")
        for location in butlerLocation.getLocations():
            with open(location, 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

#################
# Object Mapper #
#################


class MapperTestCfg(dp.Policy, yaml.YAMLObject):
    yaml_tag = u"!MapperTestCfg"

    def __init__(self, cls, root):
        super(MapperTestCfg, self).__init__({'root': root, 'cls': cls})


class MapperTest(dp.Mapper):

    @classmethod
    def cfg(cls, root=None):
        return MapperTestCfg(cls=cls, root=root)

    def __init__(self, cfg):
        super(MapperTest, self).__init__()
        # self.root = cfg['root']
        self.storage = cfg['storage']
        self.cfg = cfg

    def __repr__(self):
        return 'MapperTest(cfg=%s)' % self.cfg

    def map_str(self, dataId, write):
        template = "ccd_%(ccdNum)s.pickle"
        path = template % dataId
        if not write:
            if not self.storage.exists(path):
                return None
        location = self.storage.locationWithRoot(path)
        return dp.ButlerLocation(pythonType=PosixPickleStringHanlder, cppType=None,
                                 storageName='PickleStorage', locationList=location, dataId=dataId,
                                 mapper=self, storage=self.storage)

#####################
# Repository Mapper #
#####################


class RepoDateMapper(dp.RepositoryMapper):

    @classmethod
    def cfg(cls, policy=None, storage=None):
        # note: using RepositoryMapperCfg; there's no need for a derived cfg class.
        return dp.RepositoryMapperCfg(cls=cls, policy=policy, storage=storage)

    def getButlerLocationIfExists(self, template, dataId):
        location = template % dataId
        if self.storage.exists(location):
            return dp.ButlerLocation(
                pythonType=self.policy['repositories.cfg.python'],
                cppType=None,
                storageName=self.policy['repositories.cfg.storage'],
                locationList=(self.storage.locationWithRoot(location),),
                dataId=dataId,
                mapper=self)
        return None

    def map_cfg(self, dataId, write):
        """Map a location for a cfg file. NOTE assumes template & dataId have the key 'date', and if looking for a
        location to read, will use the registry to find the most recent date before dataId['date'].

        :param dataId: keys & values to be applied to the template.
        :param write: True if this map is being done do perform a write operation, else assumes read. Will
                      verify location exists if write is True.
        :return: a butlerLocation that describes the mapped location.
        """
        # todo check: do we need keys to complete dataId? (search Registry)
        template = self.policy['repositories.cfg.template']

        if write:
            location = template % dataId
            return ButlerLocation(
                pythonType=self.policy['repositories.cfg.python'],
                cppType=None,
                storageName=self.policy['repositories.cfg.storage'],
                locationList=(self.storage.locationWithRoot(location),),
                dataId=dataId,
                mapper=self)

        # for read mapping:
        # look for an exact match:
        butlerLoc = self.getButlerLocationIfExists(template=template, dataId=dataId)
        if butlerLoc is not None:
            return butlerLoc

        # look for a match from the next-previous date
        # note this assumes date will always be in the format yyyy-mm-dd
        dataId = dataId.copy()
        dataIdDate = datetime.datetime.strptime(dataId['date'], "%Y-%m-%d").date()
        del dataId['date']
        dateToUse = None
        lookups = self.storage.lookup(lookupProperties='date', reference=None,
                                      dataId=dataId, template=template)
        lookups.sort()
        if len(lookups) > 0:
            itr = iter(lookups)
            item = datetime.date(datetime.MINYEAR, 1, 1)
            lookups.append((datetime.date(datetime.MAXYEAR, 12, 31).strftime("%Y-%m-%d"),))
            for lookup in lookups:
                prev = item
                # we only look for 1 key so lookups ends up being a list of lists that
                # contain 1 item, so grab lookup[0]
                item = datetime.datetime.strptime(lookup[0], "%Y-%m-%d").date()
                if prev <= dataIdDate and dataIdDate < item:
                    dateToUse = prev
                    break
            if dateToUse is None:
                return None
            dataId['date'] = dateToUse.strftime("%Y-%m-%d")
            # We should be able to create a butler location from the date we found.
            butlerLoc = self.getButlerLocationIfExists(template=template, dataId=dataId)
            if butlerLoc is not None:
                return butlerLoc

        return None

########
# Test #
########


class RepoFindByDate(unittest.TestCase):

    def clean(self):
        if os.path.exists(os.path.join(ROOT, 'RepoFindByDate')):
            shutil.rmtree(os.path.join(ROOT, 'RepoFindByDate'))

    def setup(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def writeCalibs(self):
        dates = ('2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01')
        types = ('flats', 'darks')
        for date in dates:
            for type in types:
                # create a cfg of a repository for our repositories
                repoOfRepoCfg = dp.Repository.cfg(
                    mode='rw',
                    storageCfg=dp.PosixStorage.cfg(root=self.calibsRoot),
                    mapper=dp.RepositoryMapper.cfg(policy=self.repoMapperPolicy)
                )
                repoButler = dp.Butler(outputs=repoOfRepoCfg)
                # create a cfg of a repository we'd like to use. Note that we don't create the root of the cfg.
                # this will get populated by the repoOfRepos template.
                repoCfg = dp.Repository.cfg(mode='rw',
                                            storageCfg=dp.PosixStorage.cfg(),
                                            mapper=MapperTest.cfg())
                # and put that config into the repoOfRepos.
                repoButler.put(repoCfg, 'cfg', dataId={'type': type, 'date': date})
                # get the cfg back out of the butler. This will return a cfg with the root location populated.
                # i.e. repoCfg['storageCfg.root'] is populated.
                repoCfg = repoButler.get('cfg', dataId={'type': type, 'date': date}, immediate=True)
                butler = dp.Butler(outputs=repoCfg)
                obj = date + '_' + type  # object contents do not rely on date & type, but it's an easy way to verify
                butler.put(obj, 'str', {'ccdNum': 1})

    # disable this test until we work more on repo of repos; starting with DM-6227
    @unittest.expectedFailure
    def test(self):
        # create some objects that will be used when creating repositories AND when finding the created ones:
        self.calibsRoot = os.path.join(ROOT, 'RepoFindByDate')
        self.repoMapperPolicy = {
            'repositories': {
                'cfg': {
                    'template': 'cals/%(type)s/%(date)s/repoCfg.yaml',
                    'python': 'lsst.daf.persistence.RepositoryCfg',
                    'storage': 'YamlStorage'
                }
            }
        }

        # In a 'normal' case all the calibs would have been written at a previous date (or dates). For the
        # test they are created dynamically, but done in a separate function for clarity and to help ensure
        # there are no unintentionally reused objects.
        self.writeCalibs()

        # create a cfg of a repository for our repositories
        # Note that RepoDateMaper looks for the dataId['date'], and when looking for a location to read it will use the
        # registry to find the most recent date before dataId['date'].
        mapperCfg = RepoDateMapper.cfg(policy=self.repoMapperPolicy)
        repoOfRepoCfg = dp.Repository.cfg(mode='r',
                                          storageCfg=dp.PosixStorage.cfg(root=self.calibsRoot),
                                          mapper=mapperCfg)
        repoButler = dp.Butler(inputs=repoOfRepoCfg)

        TestDates = collections.namedtuple('TestDates', ('searchVal', 'expectedVal'))
        dates = (TestDates('2020-02-14', '2020-02-01'),
                 TestDates('2020-02-1', '2020-02-01'),
                 TestDates('2020-01-31', '2020-01-01'),
                 TestDates('2020-04-14', '2020-04-01'),)
        types = ('flats', 'darks')

        for date in dates:
            for type in types:
                repoCfg = repoButler.get('cfg', dataId={'type': type, 'date': date.searchVal}, immediate=True)
                butler = dp.Butler(inputs=repoCfg)
                obj = butler.get('str', {'ccdNum': 1})
                verificationDate = date.expectedVal + '_' + type
                self.assertEqual(obj, verificationDate)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
