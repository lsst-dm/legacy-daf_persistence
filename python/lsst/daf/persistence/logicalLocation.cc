#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/LogicalLocation.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_MODULE(logicalLocation, mod) {
    py::class_<LogicalLocation> cls(mod, "LogicalLocation");

    cls.def(py::init<std::string const&>());
    cls.def(py::init<std::string const&, std::shared_ptr<dafBase::PropertySet const>>());
    cls.def("locString", &LogicalLocation::locString);
    cls.def_static("setLocationMap", LogicalLocation::setLocationMap);
}

}  // persistence
}  // daf
}  // lsst
