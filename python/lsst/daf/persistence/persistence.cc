#include "pybind11/pybind11.h"

#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/python/readProxy.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_MODULE(persistence, mod) {
    py::module::import("lsst.daf.base");

    py::class_<python::ReadProxyBase, std::shared_ptr<python::ReadProxyBase>>(mod, "ReadProxyBase")
            .def(py::init<>())
            .def_readwrite("subject", &python::ReadProxyBase::subject);

}

}  // persistence
}  // daf
}  // lsst
