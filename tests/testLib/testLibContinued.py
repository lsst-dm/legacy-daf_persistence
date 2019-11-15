from lsst.utils import continueClass

from .testLib import TypeWithProxy, TypeWithoutProxy


@continueClass  # noqa F811 redefinition
class TypeWithProxy:
    def __reduce__(self):
        return (TypeWithProxy, ())


@continueClass  # noqa F811 redefinition
class TypeWithoutProxy:
    def __reduce__(self):
        return (TypeWithoutProxy, ())
