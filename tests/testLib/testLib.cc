#include "pybind11/pybind11.h"

#include "lsst/daf/base/DateTime.h"

#include "lsst/daf/persistence/python/readProxy.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {
namespace {
    class TypeWithProxy {
    };
    class TypeWithoutProxy {
    };
}

PYBIND11_PLUGIN(testLib) {
    py::module mod("testLib");

    py::class_<TypeWithProxy>(mod, "TypeWithProxy")
        .def(py::init<>());

    py::class_<TypeWithoutProxy>(mod, "TypeWithoutProxy")
        .def(py::init<>());

    python::register_proxy<base::DateTime>();
    python::register_proxy<TypeWithProxy>();

    mod.def("isValidDateTime", [](lsst::daf::base::DateTime const & dt) { return dt.isValid(); });

    return mod.ptr();
}

}}}

