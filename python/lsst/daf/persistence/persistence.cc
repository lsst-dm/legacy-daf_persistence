#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using namespace lsst::daf::persistence;

namespace py = pybind11;

PYBIND11_DECLARE_HOLDER_TYPE(MyType, std::shared_ptr<MyType>);

PYBIND11_PLUGIN(_persistence) {
    py::module mod("_persistence", "Access to the classes from the daf_persistence persistence library");

    py::class_<Persistence, std::shared_ptr<Persistence>, lsst::daf::base::Citizen> cls(mod, "Persistence");

    cls.def("getPersistStorage", &Persistence::getPersistStorage);
    cls.def("getRetrieveStorage", &Persistence::getRetrieveStorage);
    cls.def("persist", &Persistence::persist);
    cls.def("retrieve", &Persistence::retrieve);
    cls.def("unsafeRetrieve", &Persistence::unsafeRetrieve);
    cls.def_static("getPersistence", &Persistence::getPersistence);

    return mod.ptr();
}

