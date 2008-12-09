// -*- lsst-c++ -*-


/** \file
 * \brief Implementation of DataPropertyFormatter class
 *
 * \author $Author: ktlim $
 * \version $Revision: 2151 $
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/DataPropertyFormatter.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/mpi/packed_oarchive.hpp>
#include <boost/mpi/packed_iarchive.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include <lsst/daf/base/DataProperty.h>
#include "lsst/daf/base/DateTime.h"
#include "lsst/daf/persistence/FormatterImpl.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/XmlStorage.h"
#include <lsst/pex/exceptions.h>
#include <lsst/pex/logging/Trace.h>
#include <lsst/pex/policy/Policy.h>


#define EXEC_TRACE  20
static void execTrace(std::string s, int level = EXEC_TRACE) {
    lsst::pex::logging::Trace("daf.data.DataPropertyFormatter", level, s);
}

namespace lsst {
namespace daf {
namespace persistence {

using boost::serialization::make_nvp;
using lsst::daf::base::DataProperty;

/** Register this Formatter subclass through a static instance of
 * FormatterRegistration.
 */
lsst::daf::persistence::FormatterRegistration
    DataPropertyFormatter::registration("DataProperty", typeid(DataProperty),
                                        createInstance);

/** Constructor.
 * \param[in] policy Policy for configuring this Formatter
 */
DataPropertyFormatter::DataPropertyFormatter(
    lsst::pex::policy::Policy::Ptr policy) :
    Formatter(typeid(*this)), _policy(policy) {
}

/** Minimal destructor.
 */
DataPropertyFormatter::~DataPropertyFormatter(void) {
}

void DataPropertyFormatter::write(lsst::daf::base::Persistable const* persistable,
                                  Storage::Ptr storage,
                                  DataProperty::PtrType additionalData) {
    execTrace("DataPropertyFormatter write start");
    DataProperty const* dp = dynamic_cast<DataProperty const*>(persistable);
    if (dp == 0) {
        throw lsst::pex::exceptions::Runtime("Persisting non-DataProperty");
    }
    if (typeid(*storage) == typeid(BoostStorage)) {
        execTrace("DataPropertyFormatter write BoostStorage");
        BoostStorage* boost = dynamic_cast<BoostStorage*>(storage.get());
        boost->getOArchive() & *dp;
        execTrace("DataPropertyFormatter write end");
        return;
    }
    else if (typeid(*storage) == typeid(XmlStorage)) {
        execTrace("DataPropertyFormatter write XmlStorage");
        XmlStorage* xml = dynamic_cast<XmlStorage*>(storage.get());
        xml->getOArchive() & make_nvp("dataProperty", *dp);
        execTrace("DataPropertyFormatter write end");
        return;
    }
    else if (typeid(*storage) == typeid(DbStorage)) {
        execTrace("DataPropertyFormatter write DbStorage");
        DbStorage* db = dynamic_cast<DbStorage*>(storage.get());

        std::string itemName = boost::any_cast<std::string>(
            additionalData->findUnique("itemName")->getValue());
        std::string tableName = itemName;
        lsst::pex::policy::Policy::Ptr itemPolicy;
        if (_policy && _policy->exists(itemName)) {
            itemPolicy = _policy->getPolicy(itemName);
            if (itemPolicy->exists("TableName")) {
                tableName = itemPolicy->getString("TableName");
            }
        }
        db->setTableForInsert(tableName);

        std::vector<std::string> list;
        if (itemPolicy && itemPolicy->exists("KeyList")) {
            lsst::pex::policy::Policy::StringPtrArray const& array(
                itemPolicy->getStringArray("KeyList"));
            for (lsst::pex::policy::Policy::StringPtrArray::const_iterator it = array.begin();
                 it != array.end(); ++it) {
                list.push_back(**it);
            }
        }
        else {
            DataProperty::iteratorRangeType range = dp->getChildren();
            for (DataProperty::ContainerIteratorType it = range.first;
                 it != range.second; ++it) {
                list.push_back((*it)->getName());
            }
        }

        for (std::vector<std::string>::const_iterator it = list.begin();
             it != list.end(); ++it) {
            std::string::size_type split = it->find('=');
            std::string colName;
            std::string key;
            if (split == std::string::npos) {
                colName = key = *it;
            }
            else {
                colName = it->substr(0, split);
                key = it->substr(split + 1);
            }

            DataProperty::PtrType item =
                const_cast<DataProperty*>(dp)->findUnique(key);
            if (!item) {
                db->setColumnToNull(colName);
                continue;
            }

            boost::any const& value(item->getValue());
            std::type_info const& type(value.type());

            if (type == typeid(bool)) {
                db->setColumn<bool>(colName, boost::any_cast<bool>(value));
            }
            else if (type == typeid(int)) {
                db->setColumn<int>(colName, boost::any_cast<int>(value));
            }
            else if (type == typeid(long)) {
                db->setColumn<long>(
                    colName, boost::any_cast<long>(value));
            }
            else if (type == typeid(long long)) {
                db->setColumn<long long>(
                    colName, boost::any_cast<long long>(value));
            }
            else if (type == typeid(float)) {
                db->setColumn<float>(colName, boost::any_cast<float>(value));
            }
            else if (type == typeid(double)) {
                db->setColumn<double>(colName, boost::any_cast<double>(value));
            }
            else if (type == typeid(std::string)) {
                db->setColumn<std::string>(
                    colName, boost::any_cast<std::string>(value));
            }
            else if (type == typeid(lsst::daf::base::DateTime)) {
                db->setColumn<lsst::daf::base::DateTime>(
                    colName,
                    boost::any_cast<lsst::daf::base::DateTime>(value));
            }
            else {
                throw lsst::pex::exceptions::Runtime(
                    std::string("Unknown type ") + type.name() +
                    " in DataPropertyFormatter write");
            }
        }
        db->insertRow();
        execTrace("DataPropertyFormatter write end");
        return;
    }

    throw lsst::pex::exceptions::Runtime("Unrecognized Storage for DataProperty");
}

lsst::daf::base::Persistable* DataPropertyFormatter::read(
    Storage::Ptr storage, DataProperty::PtrType additionalData) {
    execTrace("DataPropertyFormatter read start");
    DataProperty* dp = new DataProperty;
    if (typeid(*storage) == typeid(BoostStorage)) {
        execTrace("DataPropertyFormatter read BoostStorage");
        BoostStorage* boost = dynamic_cast<BoostStorage*>(storage.get());
        boost->getIArchive() & *dp;
        execTrace("DataPropertyFormatter read end");
        return dp;
    }
    else if (typeid(*storage) == typeid(XmlStorage)) {
        execTrace("DataPropertyFormatter read XmlStorage");
        XmlStorage* xml = dynamic_cast<XmlStorage*>(storage.get());
        xml->getIArchive() & make_nvp("dataProperty", *dp);
        execTrace("DataPropertyFormatter read end");
        return dp;
    }
    throw lsst::pex::exceptions::Runtime("Unrecognized Storage for DataProperty");
}

void DataPropertyFormatter::update(lsst::daf::base::Persistable* persistable,
                                   Storage::Ptr storage,
                                   DataProperty::PtrType additionalData) {
    throw lsst::pex::exceptions::Runtime("Unexpected call to update for DataProperty");
}

/** Serialize a DataProperty value.
 * \param[in,out] ar Boost archive
 * \param[in,out] value Reference to boost::any value in the DataProperty
 */
template <class Archive, typename T>
static void serializeItem(Archive& ar, boost::any& value) {
    T aux;
    if (Archive::is_saving::value) {
        T aux(boost::any_cast<T>(value));
        ar & make_nvp("value", aux);
    }
    else {
        ar & make_nvp("value", aux);
        value = aux;
    }
}

/** Serialize a DataProperty to a Boost archive.  Handles text or XML
 * archives, input or output.
 * \param[in,out] ar Boost archive
 * \param[in] version Version of the DataProperty class
 * \param[in,out] persistable Pointer to the DataProperty as a Persistable
 */
template <class Archive>
void DataPropertyFormatter::delegateSerialize(
    Archive& ar, unsigned int const version, lsst::daf::base::Persistable* persistable) {
    execTrace("DataPropertyFormatter delegateSerialize start");
    DataProperty* dp = dynamic_cast<DataProperty*>(persistable);
    if (dp == 0) {
        throw lsst::pex::exceptions::Runtime("Serializing non-DataProperty");
    }
    ar & make_nvp("base", boost::serialization::base_object<lsst::daf::base::Persistable>(*dp));
    ar & make_nvp("name", dp->_name) & make_nvp("isANode", dp->_isANode);
    if (dp->_isANode) {
        execTrace("DataPropertyFormatter processing collection");
        ar & make_nvp("collection", dp->_collectionValue);
        execTrace("DataPropertyFormatter processed collection");
    }
    else {
        char type;
        if (Archive::is_saving::value) {
            std::type_info const& id(dp->_value.type());
            if (id == typeid(bool)) type = 'b';
            else if (id == typeid(int)) type = 'i';
            else if (id == typeid(double)) type = 'd';
            else if (id == typeid(std::string)) type = 's';
            else if (id == typeid(lsst::daf::base::Persistable::Ptr)) type = 'p';
            else {
                throw lsst::pex::exceptions::Runtime(
                    std::string("Unknown type in DataProperty boost::any") +
                    id.name());
            }
        }

        ar & make_nvp("type", type);

        switch (type) {
        case 'b': serializeItem<Archive, bool>(ar, dp->_value); break;
        case 'i': serializeItem<Archive, int>(ar, dp->_value); break;
        case 'd': serializeItem<Archive, double>(ar, dp->_value); break;
        case 's': serializeItem<Archive, std::string>(ar, dp->_value); break;
        case 'p': serializeItem<Archive, lsst::daf::base::Persistable::Ptr>(ar, dp->_value); break;
        default:
                  throw lsst::pex::exceptions::Runtime(
                      std::string("Unknown type reading DataProperty") + type);
        }
        execTrace("DataPropertyFormatter processed " + type);
    }
    execTrace("DataPropertyFormatter delegateSerialize end");
}

template
void lsst::daf::persistence::DataPropertyFormatter::delegateSerialize<boost::mpi::packed_oarchive>(
        boost::mpi::packed_oarchive& ar,
        unsigned int const version,
        lsst::daf::base::Persistable* persistable);

template
void lsst::daf::persistence::DataPropertyFormatter::delegateSerialize<boost::mpi::packed_iarchive>(
        boost::mpi::packed_iarchive& ar,
        unsigned int const version,
        lsst::daf::base::Persistable* persistable);


/** Factory method for DataPropertyFormatter.
 * \param[in] policy Policy for configuring the DataPropertyFormatter
 * \return Shared pointer to a new instance
 */
Formatter::Ptr DataPropertyFormatter::createInstance(
    lsst::pex::policy::Policy::Ptr policy) {
    return Formatter::Ptr(new DataPropertyFormatter(policy));
}

}}} // namespace lsst::daf::persistence
