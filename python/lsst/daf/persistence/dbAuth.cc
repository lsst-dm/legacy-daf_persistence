#include "pybind11/pybind11.h"

#include "lsst/daf/persistence/DbAuth.h"

namespace py = pybind11;

namespace lsst {
namespace daf {
namespace persistence {

PYBIND11_MODULE(dbAuth, mod) {
    py::class_<DbAuth> cls(mod, "DbAuth");

    cls.def_static("setPolicy", &DbAuth::setPolicy);
    cls.def_static("resetPolicy", &DbAuth::resetPolicy);
    cls.def_static("available", &DbAuth::available);
    cls.def_static("authString", &DbAuth::authString);
    cls.def_static("username", &DbAuth::username);
    cls.def_static("password", &DbAuth::password);
}

}  // persistence
}  // daf
}  // lsst
