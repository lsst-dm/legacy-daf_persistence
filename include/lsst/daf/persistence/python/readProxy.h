// -*- lsst-c++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
 * 
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the LSST License Statement and 
 * the GNU General Public License along with this program.  If not, 
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
 
#ifndef LSST_DAF_PERSISTENCE_PYTHON_READPROXY_H
#define LSST_DAF_PERSISTENCE_PYTHON_READPROXY_H

#include "pybind11/pybind11.h"

namespace lsst {
namespace daf {
namespace persistence {
namespace python {

/** @class lsst::daf::persistence::python::ReadProxyBase
  * @brief Base class for lazy-loading proxy.
  *
  * This class exists purely as a base class for ReadProxy in Python.
  *
  * @ingroup daf_persistence
  */

class ReadProxyBase {
public:
    pybind11::object subject;
};

/** Register a type for (optional) lazy load.
  * 
  * When called in the pybind11 wrapper this function adds an implict conversion
  * from ReadProxy to the registered type.
  * 
  * @tparam OutputType Type to register (e.g. DateTime)
  */

template <typename OutputType> void register_proxy() {
    namespace py = pybind11;
    auto implicit_caster = [](PyObject *obj, PyTypeObject *type) -> PyObject * {
        if (!py::detail::make_caster<lsst::daf::persistence::python::ReadProxyBase>().load(obj, false)) {
            return nullptr;
        }
        PyObject *result = PyObject_GetAttrString(obj, "__subject__");
        if (result == nullptr) {
            PyErr_Clear();  // needed to fall through to next conversion
        }
        return result;
    };

    if (auto tinfo = py::detail::get_type_info(typeid(OutputType))) {
        tinfo->implicit_conversions.push_back(implicit_caster);
    } else {
        py::pybind11_fail("register_proxy: Unable to find type " + py::type_id<OutputType>());
    }
}

}}}}

#endif
