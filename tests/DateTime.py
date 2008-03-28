#!/usr/bin/env python

from lsst.mwi.persistence import DateTime

ts = DateTime(45205.125)
assert ts.nsecs() == 399006000000000000L
assert ts.utc2mjd() == 45205.125
assert ts.utc2tai().nsecs() == 399005979000000000L

ts = DateTime(1192755473000000000L)
assert ts.nsecs() == 1192755473000000000L
assert ts.utc2mjd() == 54392.040196759262
assert ts.utc2tai().nsecs() == 1192755440000000000L

ts = DateTime(47892.0)
assert ts.nsecs() == 631152000000000000L
assert ts.utc2mjd() == 47892.0
assert ts.utc2tai().nsecs() == 631151975000000000L

ts = DateTime(631151998000000000L)
assert ts.utc2tai().nsecs() == 631151974000000000L

