#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_PLUGIN(storage) {
    py::module::import("lsst.daf.base");

    py::module mod("storage");

    py::class_<Storage, std::shared_ptr<Storage>, base::Citizen> cls(mod, "Storage");

    return mod.ptr();
}

}  // persistence
}  // daf
}  // lsst

