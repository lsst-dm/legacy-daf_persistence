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
import os
import shutil
import unittest

import yaml

import lsst.utils.tests
import lsst.daf.persistence as dp
from lsst.daf.persistence import Policy

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


class MapperTestCfg(Policy, yaml.YAMLObject):
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
        template = "strfile_%(strId)s.pickle"
        path = template % dataId
        if not write:
            if not self.storage.exists(path):
                return None
        location = self.storage.locationWithRoot(path)
        return dp.ButlerLocation(pythonType=PosixPickleStringHanlder, cppType=None,
                                 storageName='PickleStorage', locationList=location,
                                 dataId=dataId, mapper=self, storage=self.storage)


class ReposInButler(unittest.TestCase):

    def clean(self):
        if os.path.exists(os.path.join(ROOT, 'repoOfRepos')):
            shutil.rmtree(os.path.join(ROOT, 'repoOfRepos'))

    def setup(self):
        self.clean()

    def tearDown(self):
        self.clean()

    # disable this test until we work more on repo of repos; starting with DM-6227
    @unittest.expectedFailure
    def test(self):
        repoMapperPolicy = {
            'repositories': {
                'cfg': {
                    'template': 'repos/repo_v%(version)s/repoCfg.yaml',
                    'python': 'lsst.daf.persistence.RepositoryCfg',
                    'storage': 'YamlStorage'
                }
            }
        }

        # create a cfg of a repository for our repositories
        storageCfg = dp.PosixStorage.cfg(root=os.path.join(ROOT, 'repoOfRepos'))
        accessCfg = dp.Access.cfg(storageCfg=storageCfg)
        mapperCfg = dp.RepositoryMapper.cfg(policy=repoMapperPolicy)
        # Note that right now a repo is either input OR output, there is no input-output repo, this design
        # is result of butler design conversations. Right now, if a user wants to write to and then read from
        # a repo, a repo can have a parent repo with the same access (and mapper) parameters as itself.
        repoOfRepoCfg = dp.Repository.cfg(mode='rw',
                                          storageCfg=dp.PosixStorage.cfg(root=os.path.join(ROOT,
                                                                                           'repoOfRepos')),
                                          mapper=dp.RepositoryMapper.cfg(policy=repoMapperPolicy))

        repoButler = dp.Butler(outputs=repoOfRepoCfg)
        # create a cfg of a repository we'd like to use. Note that we don't create the root of the cfg.
        # this will get populated by the repoOfRepos template.
        repoCfg = dp.Repository.cfg(mode='rw', storageCfg=dp.PosixStorage.cfg(), mapper=MapperTest.cfg())
        # and put that config into the repoOfRep
        repoButler.put(repoCfg, 'cfg', dataId={'version': 123})

        # get the cfg back out of the butler. This will return a cfg with the root location populated.
        # i.e. repoCfg['accessCfg.storageCfg.root'] is populated.
        repoCfg = repoButler.get('cfg', dataId={'version': 123}, immediate=True)
        butler = dp.Butler(outputs=repoCfg)

        obj = 'abc'
        butler.put(obj, 'str', {'strId': 'a'})
        reloadedObj = butler.get('str', {'strId': 'a'})
        self.assertEqual(obj, reloadedObj)
        self.assertIsNot(obj, reloadedObj)  # reloaded object should be a new instance.

        # explicitly release some objects; these names will be reused momentarily.
        del butler, repoCfg, obj, reloadedObj

        # Create another repository, and put it in the repo of repos, with a new version number.
        repoCfg = dp.Repository.cfg(mode='rw', storageCfg=dp.PosixStorage.cfg(), mapper=MapperTest.cfg())
        repoButler.put(repoCfg, 'cfg', dataId={'version': 124})
        repoCfg = repoButler.get('cfg', dataId={'version': 124}, immediate=True)
        butler = dp.Butler(outputs=repoCfg)
        # create an object that is slightly different than the object in repo version 123, and give it the
        # same dataId as the object in repo 123. Put it, and get it to verify.
        obj = 'abcd'
        butler.put(obj, 'str', {'strId': 'a'})
        reloadedObj = butler.get('str', {'strId': 'a'})
        self.assertEqual(obj, reloadedObj)
        self.assertIsNot(obj, reloadedObj)

        # release the objects for repo version 124
        del butler, repoCfg, obj, reloadedObj

        # from the repo butler, get the cfgs for both repo versions, create butlers from each of them, get
        # the objects, and verify correct values.
        repo123Cfg = repoButler.get('cfg', dataId={'version': 123}, immediate=True)
        repo124Cfg = repoButler.get('cfg', dataId={'version': 124}, immediate=True)
        butler123 = dp.Butler(inputs=repo123Cfg)
        butler124 = dp.Butler(inputs=repo124Cfg)
        obj123 = butler123.get('str', {'strId': 'a'})
        obj124 = butler124.get('str', {'strId': 'a'})
        self.assertEqual(obj123, 'abc')
        self.assertEqual(obj124, 'abcd')


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
