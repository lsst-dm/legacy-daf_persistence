// -*- lsst-c++ -*-
%define persistenceLib_DOCSTRING
"
Access to the lsst::daf::persistence classes
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.daf.persistence", docstring=persistenceLib_DOCSTRING) persistenceLib

%{
#include "lsst/daf/base.h"
#include "lsst/daf/persistence/DbAuth.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/DbStorage.h"
%}

%inline %{
namespace lsst { namespace daf { namespace persistence { } } }
namespace boost {
    namespace filesystem { }
    class bad_any_cast;
}

using namespace lsst::daf::persistence;
%}

%include "lsst/p_lsstSwig.i"

%include "lsst/daf/base/Citizen.h"

%import "lsst/daf/base/DateTime.h"
%import "lsst/daf/base/Persistable.h"
%import "lsst/daf/base/DataProperty.h"
%include "lsst/daf/persistence/DbAuth.h"
%include "lsst/daf/persistence/LogicalLocation.h"

%newobject lsst::daf::persistence::Persistence::getPersistence;
%newobject lsst::daf::persistence::Persistence::getPersistStorage;
%newobject lsst::daf::persistence::Persistence::getRetrieveStorage;
%newobject lsst::daf::persistence::Persistence::unsafeRetrieve;
%include "lsst/daf/persistence/Persistence.h"

%include "lsst/daf/persistence/Storage.h"
%include "lsst/daf/persistence/DbStorage.h"

// Next two needed for typedefs.
%import "lsst/daf/base/DataProperty.h"
%import "lsst/pex/policy/Policy.h"
typedef long long int64_t;

%boost_shared_ptr(PersistenceSharedPtr, lsst::daf::persistence::Persistence);
%boost_shared_ptr(StorageSharedPtr, lsst::daf::persistence::Storage);
%template(StorageList) std::vector<boost::shared_ptr<lsst::daf::persistence::Storage> >;
%template(TableList) std::vector<std::string>;

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
