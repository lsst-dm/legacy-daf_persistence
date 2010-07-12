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
 
%define persistenceLib_DOCSTRING
"
Access to the lsst::daf::persistence classes
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.daf.persistence", docstring=persistenceLib_DOCSTRING) persistenceLib

%{
#include "lsst/daf/persistence/DbAuth.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/DbTsvStorage.h"
%}

%include "lsst/p_lsstSwig.i"

%lsst_exceptions();

SWIG_SHARED_PTR(Persistence, lsst::daf::persistence::Persistence)
SWIG_SHARED_PTR(LogicalLocation, lsst::daf::persistence::LogicalLocation)
SWIG_SHARED_PTR(Storage, lsst::daf::persistence::Storage)
SWIG_SHARED_PTR_DERIVED(DbStorage, lsst::daf::persistence::Storage, lsst::daf::persistence::DbStorage)
SWIG_SHARED_PTR_DERIVED(DbTsvStorage, lsst::daf::persistence::DbStorage, lsst::daf::persistence::DbTsvStorage)

%import "lsst/pex/exceptions/exceptionsLib.i"
%import "lsst/daf/base/baseLib.i"
%import "lsst/pex/logging/loggingLib.i"
%import "lsst/pex/policy/policyLib.i"

%newobject lsst::daf::persistence::Persistence::getPersistence;
%newobject lsst::daf::persistence::Persistence::getPersistStorage;
%newobject lsst::daf::persistence::Persistence::getRetrieveStorage;
%newobject lsst::daf::persistence::Persistence::unsafeRetrieve;

%template(StorageList) std::vector<boost::shared_ptr<lsst::daf::persistence::Storage> >;
%template(TableList) std::vector<std::string>;

%include "lsst/daf/persistence/DbAuth.h"
%include "lsst/daf/persistence/LogicalLocation.h"
%include "lsst/daf/persistence/Storage.h"
%include "lsst/daf/persistence/DbStorage.h"
%include "lsst/daf/persistence/DbTsvStorage.h"
%include "lsst/daf/persistence/Persistence.h"

typedef long long int64_t;

%extend lsst::daf::persistence::DbStorage {

    %template(setColumnChar) setColumn<char>;
    %template(setColumnShort) setColumn<short>;
    %template(setColumnInt) setColumn<int>;
    %template(setColumnLong) setColumn<long>;
    %template(setColumnInt64) setColumn<int64_t>;
    %template(setColumnFloat) setColumn<float>;
    %template(setColumnDouble) setColumn<double>;
    %template(setColumnString) setColumn<std::string>;
    %template(setColumnBool) setColumn<bool>;

    %template(condParamChar) condParam<char>;
    %template(condParamShort) condParam<short>;
    %template(condParamInt) condParam<int>;
    %template(condParamLong) condParam<long>;
    %template(condParamInt64) condParam<int64_t>;
    %template(condParamFloat) condParam<float>;
    %template(condParamDouble) condParam<double>;
    %template(condParamString) condParam<std::string>;
    %template(condParamBool) condParam<bool>;

    %template(getColumnByPosChar) getColumnByPos<char>;
    %template(getColumnByPosShort) getColumnByPos<short>;
    %template(getColumnByPosInt) getColumnByPos<int>;
    %template(getColumnByPosLong) getColumnByPos<long>;
    %template(getColumnByPosInt64) getColumnByPos<int64_t>;
    %template(getColumnByPosFloat) getColumnByPos<float>;
    %template(getColumnByPosDouble) getColumnByPos<double>;
    %template(getColumnByPosString) getColumnByPos<std::string>;
    %template(getColumnByPosBool) getColumnByPos<bool>;
}
