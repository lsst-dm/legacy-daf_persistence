#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/FormatterStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_MODULE(formatterStorage, mod) {
    py::module::import("lsst.daf.base");

    py::class_<FormatterStorage, std::shared_ptr<FormatterStorage>, base::Citizen> cls(mod, "FormatterStorage");
}

}  // persistence
}  // daf
}  // lsst
