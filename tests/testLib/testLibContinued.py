from __future__ import absolute_import

from lsst.utils import continueClass

from .testLib import TypeWithProxy, TypeWithoutProxy

@continueClass
class TypeWithProxy:
    def __reduce__(self):
        return (TypeWithProxy, ())

@continueClass
class TypeWithoutProxy:
    def __reduce__(self):
        return (TypeWithoutProxy, ())
