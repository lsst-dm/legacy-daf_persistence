#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/StorageFormatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_PLUGIN(storageFormatter) {
    py::module::import("lsst.daf.base");

    py::module mod("storageFormatter");

    py::class_<StorageFormatter, std::shared_ptr<StorageFormatter>, base::Citizen> cls(mod, "StorageFormatter");

    return mod.ptr();
}

}  // persistence
}  // daf
}  // lsst
