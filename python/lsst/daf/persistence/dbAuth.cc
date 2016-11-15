#include <pybind11/pybind11.h>

#include "lsst/daf/persistence/DbAuth.h"

using namespace lsst::daf::persistence;

namespace py = pybind11;

PYBIND11_DECLARE_HOLDER_TYPE(MyType, std::shared_ptr<MyType>);

PYBIND11_PLUGIN(_dbAuth) {
    py::module mod("_dbAuth", "Access to the classes from the daf_persistence dbAuth library");

    py::class_<DbAuth> cls(mod, "DbAuth");

    cls.def_static("setPolicy", &DbAuth::setPolicy);
    cls.def_static("resetPolicy", &DbAuth::resetPolicy);
    cls.def_static("available", &DbAuth::available);
    cls.def_static("authString", &DbAuth::authString);
    cls.def_static("username", &DbAuth::username);
    cls.def_static("password", &DbAuth::password);

    return mod.ptr();
}

