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

import unittest

import lsst.utils.tests as utilsTests
import lsst.daf.persistence as dp

class TestCfgRelationship(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def testRWModes(self):
		# inputs must be read-only or read-write and not write-only
		cfg = dp.Repository.cfg(mode='r')
		butler = dp.Butler(inputs=cfg)
		cfg = dp.Repository.cfg(mode='rw')
		butler = dp.Butler(inputs=cfg)
		cfg = dp.Repository.cfg(mode='w')
		self.assertRaises(RuntimeError, dp.Butler, inputs=cfg)

		# outputs must be write-only or read-write and not read-only
		cfg = dp.Repository.cfg(mode='w')
		butler = dp.Butler(outputs=cfg)
		cfg = dp.Repository.cfg(mode='rw')
		butler = dp.Butler(outputs=cfg)
		cfg = dp.Repository.cfg(mode='r')
		self.assertRaises(RuntimeError, dp.Butler, outputs=cfg)


	def testExistingParents(self):
		# parents of inputs should be added to the inputs list
		cfgA = dp.Repository.cfg(mode='r')
		cfgB = dp.Repository.cfg(mode='r', parentCfgs=cfgA)
		butler = dp.Butler(inputs=cfgB)
		self.assertEqual(len(butler.inputs), 2)
		# verify serach order:
		self.assertEqual(butler.inputs[0].cfg, cfgB)
		self.assertEqual(butler.inputs[1].cfg, cfgA)
		self.assertEqual(len(butler.outputs), 0)

		# parents of readable outputs should be added to the inputs list
		cfgA = dp.Repository.cfg(mode='r')
		cfgB = dp.Repository.cfg(mode='rw', parentCfgs=cfgA)
		butler = dp.Butler(outputs=cfgB)
		self.assertEqual(len(butler.inputs), 2)
		# verify serach order:
		self.assertEqual(butler.inputs[0].cfg, cfgB)
		self.assertEqual(butler.inputs[1].cfg, cfgA)
		self.assertEqual(len(butler.outputs), 1)
		self.assertEqual(butler.outputs[0].cfg, cfgB)

		# if an output repository is write-only its parents should not be added to the inputs.
		cfgA = dp.Repository.cfg(mode='r')
		cfgB = dp.Repository.cfg(mode='w', parentCfgs=cfgA)
		butler = dp.Butler(outputs=cfgB)
		self.assertEqual(len(butler.inputs), 0)
		self.assertEqual(len(butler.outputs), 1)
		self.assertEqual(butler.outputs[0].cfg, cfgB)

	def testInputsOrderAndTagging(self):
		# input A has parents B and C. input D has parents E and F. 
		# Search order should be A, B, C, D, E, F
		cfgC = dp.Repository.cfg(mode='r', tags='configC')
		cfgB = dp.Repository.cfg(mode='r', tags='configB')
		cfgA = dp.Repository.cfg(mode='r', parentCfgs=[cfgB, cfgC], tags='configA')
		cfgF = dp.Repository.cfg(mode='r', tags='configF')
		cfgE = dp.Repository.cfg(mode='r', tags='configE')
		cfgD = dp.Repository.cfg(mode='r', parentCfgs=[cfgE, cfgF], tags='configD')
		butler = dp.Butler(inputs=[cfgA, cfgD])
		self.assertEqual(len(butler.inputs), 6)
		# verify serach order:
		self.assertEqual(butler.inputs[0].cfg, cfgA)
		self.assertEqual(butler.inputs[1].cfg, cfgB)
		self.assertEqual(butler.inputs[2].cfg, cfgC)
		self.assertEqual(butler.inputs[3].cfg, cfgD)
		self.assertEqual(butler.inputs[4].cfg, cfgE)
		self.assertEqual(butler.inputs[5].cfg, cfgF)

		# A has parents B and C, D has parents E and C. 
		# Search order should be A, B, C, D, E
		# C should get tagged with both repos that it's a parent of: A and D.
		cfgC = dp.Repository.cfg(mode='r', tags='configC')
		cfgB = dp.Repository.cfg(mode='r', tags='configB')
		cfgA = dp.Repository.cfg(mode='r', tags='configA', parentCfgs=[cfgB, cfgC])
		cfgE = dp.Repository.cfg(mode='r', tags='configE')
		cfgD = dp.Repository.cfg(mode='r', tags='configD', parentCfgs=[cfgE, cfgC])
		butler = dp.Butler(inputs=[cfgA, cfgD])
		self.assertEqual(len(butler.inputs), 5)
		# verify serach order:
		self.assertEqual(butler.inputs[0].cfg, cfgA)
		self.assertEqual(butler.inputs[0].tags, set(['configA']))
		self.assertEqual(butler.inputs[1].cfg, cfgB)
		self.assertEqual(butler.inputs[1].tags, set(['configA', 'configB']))
		self.assertEqual(butler.inputs[2].cfg, cfgC)
		self.assertEqual(butler.inputs[2].tags, set(['configA', 'configC', 'configD']))
		self.assertEqual(butler.inputs[3].cfg, cfgD)
		self.assertEqual(butler.inputs[3].tags, set(['configD']))
		self.assertEqual(butler.inputs[4].cfg, cfgE)
		self.assertEqual(butler.inputs[4].tags, set(['configD', 'configE']))

		# A has parent B, B has parents C and D. E has parent F. 
		# search order should be A, B, C, D, E, F
		cfgD = dp.Repository.cfg(mode='r', tags='configD')
		cfgC = dp.Repository.cfg(mode='r', tags='configC')
		cfgB = dp.Repository.cfg(mode='r', tags='configB', parentCfgs=[cfgC, cfgD])
		cfgA = dp.Repository.cfg(mode='r', tags='configA', parentCfgs=cfgB)
		cfgF = dp.Repository.cfg(mode='r', tags='configF')
		cfgE = dp.Repository.cfg(mode='r', tags='configE', parentCfgs=cfgF)
		butler = dp.Butler(inputs=[cfgA, cfgE])
		self.assertEqual(len(butler.inputs), 6)
		# verify serach order:
		self.assertEqual(butler.inputs[0].cfg, cfgA)
		self.assertEqual(butler.inputs[1].cfg, cfgB)
		self.assertEqual(butler.inputs[2].cfg, cfgC)
		self.assertEqual(butler.inputs[3].cfg, cfgD)
		self.assertEqual(butler.inputs[4].cfg, cfgE)
		self.assertEqual(butler.inputs[5].cfg, cfgF)

		self.assertEqual(butler.inputs[0].tags, set(['configA']))
		self.assertEqual(butler.inputs[1].tags, set(['configA', 'configB']))
		self.assertEqual(butler.inputs[2].tags, set(['configA', 'configB', 'configC']))
		self.assertEqual(butler.inputs[3].tags, set(['configA', 'configB', 'configD']))
		self.assertEqual(butler.inputs[4].tags, set(['configE']))
		self.assertEqual(butler.inputs[5].tags, set(['configE', 'configF']))


def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(TestCfgRelationship)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
