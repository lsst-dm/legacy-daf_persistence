#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "lsst/daf/persistence/Storage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using namespace lsst::daf::persistence;

namespace py = pybind11;

template <typename T, typename C>
void declareParams(C & cls, const std::string & suffix) {
	cls.def(("setColumn" + suffix).c_str(), &DbStorage::setColumn<T>);
	cls.def(("condParam" + suffix).c_str(), &DbStorage::condParam<T>);
	cls.def(("outParam" + suffix).c_str(), &DbStorage::outParam<T>);
	cls.def(("getColumnByPos" + suffix).c_str(), &DbStorage::getColumnByPos<T>);
}

PYBIND11_DECLARE_HOLDER_TYPE(MyType, std::shared_ptr<MyType>);

PYBIND11_PLUGIN(_dbStorage) {
    py::module mod("_dbStorage", "Access to the classes from the daf_persistence dbStorage library");

    py::class_<DbStorage, std::shared_ptr<DbStorage>, Storage> cls(mod, "DbStorage");

	/* Constructors */
	cls.def(py::init<>());

	/* Member functions */
	cls.def("setPolicy", &DbStorage::setPolicy);
	cls.def("setPersistLocation", &DbStorage::setPersistLocation);
	cls.def("setRetrieveLocation", &DbStorage::setRetrieveLocation);
	cls.def("startTransaction", &DbStorage::startTransaction);
	cls.def("endTransaction", &DbStorage::endTransaction);
	cls.def("createTableFromTemplate", &DbStorage::createTableFromTemplate,
		py::arg("tableName"), py::arg("templateName"), py::arg("mayAlreadyExist")=false);
	cls.def("dropTable", &DbStorage::dropTable);
	cls.def("truncateTable", &DbStorage::truncateTable);
	cls.def("executeSql", &DbStorage::executeSql);
	cls.def("setTableForInsert", &DbStorage::setTableForInsert);
	cls.def("setColumnToNull", &DbStorage::setColumnToNull);
	cls.def("insertRow", &DbStorage::insertRow);
	cls.def("setTableForQuery", &DbStorage::setTableForQuery,
		py::arg("tableName"), py::arg("isExpr")=false);
	cls.def("setTableListForQuery", &DbStorage::setTableListForQuery);
	cls.def("outColumn", &DbStorage::outColumn,
		py::arg("columnName"), py::arg("isExpr")=false);
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

