#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "lsst/daf/persistence/FormatterStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

namespace py = pybind11;
using namespace pybind11::literals;

namespace lsst {
namespace daf {
namespace persistence {
namespace {

template <typename T, typename C>
void declareParams(C& cls, const std::string& suffix) {
    cls.def(("setColumn" + suffix).c_str(), &DbStorage::setColumn<T>);
    cls.def(("condParam" + suffix).c_str(), &DbStorage::condParam<T>);
    cls.def(("outParam" + suffix).c_str(), &DbStorage::outParam<T>);
    cls.def(("getColumnByPos" + suffix).c_str(), &DbStorage::getColumnByPos<T>);
}

}  // <anonymous>

PYBIND11_PLUGIN(dbStorage) {
    py::module mod("dbStorage");

    py::class_<DbStorage, std::shared_ptr<DbStorage>, FormatterStorage> cls(mod, "DbStorage");

    /* Constructors */
    cls.def(py::init<>());

    /* Member functions */
    cls.def("setPolicy", &DbStorage::setPolicy);
    cls.def("setPersistLocation", &DbStorage::setPersistLocation);
    cls.def("setRetrieveLocation", &DbStorage::setRetrieveLocation);
    cls.def("startTransaction", &DbStorage::startTransaction);
    cls.def("endTransaction", &DbStorage::endTransaction);
    cls.def("createTableFromTemplate", &DbStorage::createTableFromTemplate, "tableName"_a, "templateName"_a,
            "mayAlreadyExist"_a = false);
    cls.def("dropTable", &DbStorage::dropTable);
    cls.def("truncateTable", &DbStorage::truncateTable);
    cls.def("executeSql", &DbStorage::executeSql);
    cls.def("setTableForInsert", &DbStorage::setTableForInsert);
    cls.def("setColumnToNull", &DbStorage::setColumnToNull);
    cls.def("insertRow", &DbStorage::insertRow);
    cls.def("setTableForQuery", &DbStorage::setTableForQuery, "tableName"_a, "isExpr"_a = false);
    cls.def("setTableListForQuery", &DbStorage::setTableListForQuery);
    cls.def("outColumn", &DbStorage::outColumn, "columnName"_a, "isExpr"_a = false);
    cls.def("orderBy", &DbStorage::orderBy);
    cls.def("groupBy", &DbStorage::groupBy);
    cls.def("setQueryWhere", &DbStorage::setQueryWhere);
    cls.def("query", &DbStorage::query);
    cls.def("next", &DbStorage::next);
    cls.def("__next__", &DbStorage::next);
    cls.def("columnIsNull", &DbStorage::columnIsNull);
    cls.def("finishQuery", &DbStorage::finishQuery);

    /* Templated member functions */
    declareParams<char>(cls, "Char");
    declareParams<short>(cls, "Short");
    declareParams<int>(cls, "Int");
    declareParams<long>(cls, "Long");
    declareParams<long long>(cls, "Int64");
    declareParams<float>(cls, "Float");
    declareParams<double>(cls, "Double");
    declareParams<std::string>(cls, "String");
    declareParams<bool>(cls, "Bool");

    return mod.ptr();
}

}  // persistence
}  // daf
}  // lsst
