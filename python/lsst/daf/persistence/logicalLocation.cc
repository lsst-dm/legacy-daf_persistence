#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using namespace lsst::daf::persistence;

namespace py = pybind11;

PYBIND11_DECLARE_HOLDER_TYPE(MyType, std::shared_ptr<MyType>);

PYBIND11_PLUGIN(_logicalLocation) {
    py::module mod("_logicalLocation", "Access to the classes from the daf_persistence logicalLocation library");

    py::class_<LogicalLocation> cls(mod, "LogicalLocation");

    cls.def(py::init<std::string const&>());
    cls.def(py::init<std::string const&, CONST_PTR(dafBase::PropertySet)>());
    cls.def("locString", &LogicalLocation::locString);
    cls.def_static("setLocationMap", LogicalLocation::setLocationMap);

    return mod.ptr();
}

