#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using namespace lsst::daf::persistence;

namespace py = pybind11;

PYBIND11_DECLARE_HOLDER_TYPE(MyType, std::shared_ptr<MyType>);

PYBIND11_PLUGIN(_storage) {
    py::module mod("_storage", "Access to the classes from the daf_persistence storage library");

    py::class_<Storage, std::shared_ptr<Storage>, lsst::daf::base::Citizen> cls(mod, "Storage");

    return mod.ptr();
}

