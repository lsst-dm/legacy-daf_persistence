#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/FormatterStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

#include "lsst/daf/persistence/python/readProxy.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_PLUGIN(persistence) {
    py::module::import("lsst.daf.base");

    py::module mod("persistence");

    py::class_<python::ReadProxyBase, std::shared_ptr<python::ReadProxyBase>>(mod, "ReadProxyBase")
            .def(py::init<>())
            .def_readwrite("subject", &python::ReadProxyBase::subject);

    py::class_<Persistence, std::shared_ptr<Persistence>, base::Citizen> clsPersistence(mod, "Persistence");

    clsPersistence.def("getPersistStorage", &Persistence::getPersistStorage);
    clsPersistence.def("getRetrieveStorage", &Persistence::getRetrieveStorage);
    clsPersistence.def("persist", &Persistence::persist);
    clsPersistence.def("retrieve", &Persistence::retrieve);
    clsPersistence.def("unsafeRetrieve", &Persistence::unsafeRetrieve);
    clsPersistence.def_static("getPersistence", &Persistence::getPersistence);

    return mod.ptr();
}

}  // persistence
}  // daf
}  // lsst
