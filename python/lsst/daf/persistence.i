// -*- lsst-c++ -*-
%define persistence_DOCSTRING
"
Access to the persistence classes from the mwi library
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.mwi", docstring=persistence_DOCSTRING) persistence

%{
#include "lsst/mwi/persistence/DateTime.h"
#include "lsst/mwi/persistence/DbAuth.h"
#include "lsst/mwi/persistence/LogicalLocation.h"
#include "lsst/mwi/persistence/Persistable.h"
#include "lsst/mwi/persistence/Persistence.h"
#include "lsst/mwi/persistence/Storage.h"
#include "lsst/mwi/persistence/DbStorage.h"
%}

%inline %{
namespace lsst { namespace mwi { namespace persistence { } } }
namespace boost { namespace filesystem { } }

using namespace lsst::mwi::persistence;
%}

%include "p_lsstSwig.i"

%include "lsst/mwi/data/Citizen.h"

%include "lsst/mwi/persistence/DateTime.h"
%include "lsst/mwi/persistence/DbAuth.h"
%include "lsst/mwi/persistence/LogicalLocation.h"
%include "lsst/mwi/persistence/Persistable.h"

%newobject lsst::mwi::persistence::Persistence::getPersistence;
%newobject lsst::mwi::persistence::Persistence::getPersistStorage;
%newobject lsst::mwi::persistence::Persistence::getRetrieveStorage;
%newobject lsst::mwi::persistence::Persistence::unsafeRetrieve;
%include "lsst/mwi/persistence/Persistence.h"

%include "lsst/mwi/persistence/Storage.h"
%include "lsst/mwi/persistence/DbStorage.h"

// Next two needed for typedefs.
%import "lsst/mwi/data/DataProperty.h"
%import "lsst/mwi/policy/Policy.h"
typedef long long int64_t;

%boost_shared_ptr(PersistableSharedPtr, lsst::mwi::persistence::Persistable)
%boost_shared_ptr(PersistenceSharedPtr, lsst::mwi::persistence::Persistence);
%boost_shared_ptr(StorageSharedPtr, lsst::mwi::persistence::Storage);
%template(StorageList) std::vector<boost::shared_ptr<lsst::mwi::persistence::Storage> >;
%template(TableList) std::vector<std::string>;

%extend lsst::mwi::persistence::DbStorage {


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
