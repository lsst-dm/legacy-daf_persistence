from lsst.utils import continueClass

from .testLib import TypeWithProxy, TypeWithoutProxy


@continueClass  # noqa: F811 (FIXME: remove for py 3.8+)
class TypeWithProxy:  # noqa: F811
    def __reduce__(self):
        return (TypeWithProxy, ())


@continueClass  # noqa: F811 (FIXME: remove for py 3.8+)
class TypeWithoutProxy:  # noqa: F811
    def __reduce__(self):
        return (TypeWithoutProxy, ())
