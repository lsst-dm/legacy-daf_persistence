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
 


/** \file
 * \brief Implementation of PropertySetFormatter class
 *
 * \version $Revision: 2151 $
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf_persistence
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/PropertySetFormatter.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/mpi/packed_oarchive.hpp>
#include <boost/mpi/packed_iarchive.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

#include <lsst/daf/base/PropertySet.h>
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
    lsst::pex::logging::Trace("daf.persistence.PropertySetFormatter", level, s);
}

namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;
namespace pexPolicy = lsst::pex::policy;

using boost::serialization::make_nvp;

/** Register this Formatter subclass through a static instance of
 * FormatterRegistration.
 */
dafPersist::FormatterRegistration
dafPersist::PropertySetFormatter::registration("PropertySet",
                                               typeid(dafBase::PropertySet),
                                               createInstance);

/** Constructor.
 * \param[in] policy Policy for configuring this Formatter
 */
dafPersist::PropertySetFormatter::PropertySetFormatter(
    pexPolicy::Policy::Ptr policy) :
    dafPersist::Formatter(typeid(*this)), _policy(policy) {
}

/** Minimal destructor.
 */
dafPersist::PropertySetFormatter::~PropertySetFormatter(void) {
}

void dafPersist::PropertySetFormatter::write(
    dafBase::Persistable const* persistable,
    dafPersist::Storage::Ptr storage,
    dafBase::PropertySet::Ptr additionalData) {
    execTrace("PropertySetFormatter write start");
    dafBase::PropertySet const* ps =
        dynamic_cast<dafBase::PropertySet const*>(persistable);
    if (ps == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Persisting non-PropertySet");
    }
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        execTrace("PropertySetFormatter write BoostStorage");
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        boost->getOArchive() & *ps;
        execTrace("PropertySetFormatter write end");
        return;
    }
    else if (typeid(*storage) == typeid(dafPersist::XmlStorage)) {
        execTrace("PropertySetFormatter write XmlStorage");
        dafPersist::XmlStorage* xml =
            dynamic_cast<dafPersist::XmlStorage*>(storage.get());
        xml->getOArchive() & make_nvp("propertySet", *ps);
        execTrace("PropertySetFormatter write end");
        return;
    }
    else if (typeid(*storage) == typeid(dafPersist::DbStorage)) {
        execTrace("PropertySetFormatter write DbStorage");
        dafPersist::DbStorage* db =
            dynamic_cast<dafPersist::DbStorage*>(storage.get());

        std::string itemName = additionalData->getAsString("itemName");
        std::string tableName = itemName;
        pexPolicy::Policy::Ptr itemPolicy;
        if (_policy && _policy->exists(itemName)) {
            itemPolicy = _policy->getPolicy(itemName);
            if (itemPolicy->exists("TableName")) {
                tableName = itemPolicy->getString("TableName");
            }
        }
        db->setTableForInsert(tableName);

        std::vector<std::string> list;
        if (itemPolicy && itemPolicy->exists("KeyList")) {
            pexPolicy::Policy::StringArray const& array(
                itemPolicy->getStringArray("KeyList"));
            for (pexPolicy::Policy::StringArray::const_iterator it =
                 array.begin(); it != array.end(); ++it) {
                list.push_back(*it);
            }
        }
        else {
            list = ps->paramNames(false);
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

            if (!ps->exists(key)) {
                db->setColumnToNull(colName);
                continue;
            }

            std::type_info const& type(ps->typeOf(key));

            if (type == typeid(bool)) {
                db->setColumn<bool>(colName, ps->get<bool>(key));
            }
            else if (type == typeid(char)) {
                db->setColumn<char>(colName, ps->get<char>(key));
            }
            else if (type == typeid(short)) {
                db->setColumn<short>(colName, ps->get<short>(key));
            }
            else if (type == typeid(int)) {
                db->setColumn<int>(colName, ps->get<int>(key));
            }
            else if (type == typeid(long)) {
                db->setColumn<long>(colName, ps->get<long>(key));
            }
            else if (type == typeid(long long)) {
                db->setColumn<long long>(colName, ps->get<long long>(key));
            }
            else if (type == typeid(float)) {
                db->setColumn<float>(colName, ps->get<float>(key));
            }
            else if (type == typeid(double)) {
                db->setColumn<double>(colName, ps->get<double>(key));
            }
            else if (type == typeid(std::string)) {
                db->setColumn<std::string>(colName, ps->get<std::string>(key));
            }
            else if (type == typeid(dafBase::DateTime)) {
                db->setColumn<dafBase::DateTime>(
                    colName, ps->get<dafBase::DateTime>(key));
            }
            else {
                throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, 
                    std::string("Unknown type ") + type.name() +
                    " in PropertySetFormatter write");
            }
        }
        db->insertRow();
        execTrace("PropertySetFormatter write end");
        return;
    }

    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Unrecognized Storage for PropertySet");
}

dafBase::Persistable* dafPersist::PropertySetFormatter::read(
    dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    execTrace("PropertySetFormatter read start");
    dafBase::PropertySet* ps = new dafBase::PropertySet;
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        execTrace("PropertySetFormatter read BoostStorage");
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        boost->getIArchive() & *ps;
        execTrace("PropertySetFormatter read end");
        return ps;
    }
    else if (typeid(*storage) == typeid(dafPersist::XmlStorage)) {
        execTrace("PropertySetFormatter read XmlStorage");
        dafPersist::XmlStorage* xml =
            dynamic_cast<dafPersist::XmlStorage*>(storage.get());
        xml->getIArchive() & make_nvp("propertySet", *ps);
        execTrace("PropertySetFormatter read end");
        return ps;
    }
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Unrecognized Storage for PropertySet");
}

void dafPersist::PropertySetFormatter::update(dafBase::Persistable* persistable,
                                   dafPersist::Storage::Ptr storage,
                                   dafBase::PropertySet::Ptr additionalData) {
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Unexpected call to update for PropertySet");
}

/** Serialize a PropertySet value.
 * \param[in,out] ar Boost archive
 * \param[in] name Name of property to serialize/deserialize.
 * \param[in,out] ps Pointer to PropertySet.
 */
template <class Archive, typename T>
static void serializeItem(Archive& ar, std::string const& name,
                          dafBase::PropertySet* ps) {
    std::vector<T> value;
    if (Archive::is_saving::value) {
        value = ps->getArray<T>(name);
        ar & make_nvp("value", value);
    }
    else {
        ar & make_nvp("value", value);
        ps->set(name, value);
    }
}

/** Serialize a DateTime PropertySet value.
 * \param[in,out] ar Boost archive
 * \param[in] name Name of property to serialize/deserialize.
 * \param[in,out] ps Pointer to PropertySet.
 */
template <class Archive>
static void serializeDateTime(Archive& ar, std::string const& name,
                              dafBase::PropertySet* ps) {
    std::vector<dafBase::DateTime> value;
    std::vector<long long> nsecs;
    if (Archive::is_saving::value) {
        value = ps->getArray<dafBase::DateTime>(name);
        for (std::vector<dafBase::DateTime>::const_iterator i = value.begin();
             i != value.end(); ++i) {
            nsecs.push_back(i->nsecs());
        }
        ar & make_nvp("value", nsecs);
    }
    else {
        ar & make_nvp("value", nsecs);
        for (std::vector<long long>::const_iterator i = nsecs.begin();
             i != nsecs.end(); ++i) {
            value.push_back(dafBase::DateTime(*i));
        }
        ps->set(name, value);
    }
}

/** Serialize a PropertySet to a Boost archive.  Handles text or XML
 * archives, input or output.
 * \param[in,out] ar Boost archive
 * \param[in] version Version of the PropertySet class
 * \param[in,out] persistable Pointer to the PropertySet as a Persistable
 */
template <class Archive>
void dafPersist::PropertySetFormatter::delegateSerialize(
    Archive& ar, unsigned int const version, dafBase::Persistable* persistable) {
    execTrace("PropertySetFormatter delegateSerialize start");
    dafBase::PropertySet* ps =
        dynamic_cast<dafBase::PropertySet*>(persistable);
    if (ps == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Serializing non-PropertySet");
    }
    ar & make_nvp("base",
                  boost::serialization::base_object<dafBase::Persistable>(*ps));
    
    std::vector<std::string> names;
    size_t nNames;
    if (Archive::is_saving::value) {
        names = ps->paramNames(false);
        nNames = names.size();
    }
    ar & make_nvp("nitems", nNames);

    char type;
    std::string name;
    for (size_t i = 0; i < nNames; ++i) {
        if (Archive::is_saving::value) {
            name = names[i];
            std::type_info const& id(ps->typeOf(name));

            if (id == typeid(bool)) type = 'b';
            else if (id == typeid(char)) type = 'c';
            else if (id == typeid(signed char)) type = 'y';
            else if (id == typeid(unsigned char)) type = 'C';
            else if (id == typeid(short)) type = 'w';
            else if (id == typeid(unsigned short)) type = 'W';
            else if (id == typeid(int)) type = 'i';
            else if (id == typeid(unsigned int)) type = 'I';
            else if (id == typeid(long)) type = 'l';
            else if (id == typeid(unsigned long)) type = 'L';
            else if (id == typeid(long long)) type = 'x';
            else if (id == typeid(unsigned long long)) type = 'X';
            else if (id == typeid(float)) type = 'f';
            else if (id == typeid(double)) type = 'd';
            else if (id == typeid(std::string)) type = 's';
            else if (id == typeid(dafBase::DateTime)) type = 'T';
            else if (id == typeid(dafBase::Persistable::Ptr)) type = 'p';
            else {
                throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException,
                                  name +
                                  ": Unknown type in PropertySet serialize");
            }
        }

        ar & make_nvp("name", name);
        ar & make_nvp("type", type);
        switch (type) {
        case 'b': serializeItem<Archive, bool>(ar, name, ps); break;
        case 'c': serializeItem<Archive, char>(ar, name, ps); break;
        case 'y': serializeItem<Archive, signed char>(ar, name, ps); break;
        case 'C': serializeItem<Archive, unsigned char>(ar, name, ps); break;
        case 'w': serializeItem<Archive, short>(ar, name, ps); break;
        case 'W': serializeItem<Archive, unsigned short>(ar, name, ps); break;
        case 'i': serializeItem<Archive, int>(ar, name, ps); break;
        case 'I': serializeItem<Archive, unsigned int>(ar, name, ps); break;
        case 'l': serializeItem<Archive, long>(ar, name, ps); break;
        case 'L': serializeItem<Archive, unsigned long>(ar, name, ps); break;
        case 'x': serializeItem<Archive, long long>(ar, name, ps); break;
        case 'X': serializeItem<Archive, unsigned long long>(ar, name, ps); break;
        case 'f': serializeItem<Archive, float>(ar, name, ps); break;
        case 'd': serializeItem<Archive, double>(ar, name, ps); break;
        case 's': serializeItem<Archive, std::string>(ar, name, ps); break;
        case 'T': serializeDateTime<Archive>(ar, name, ps); break;
        case 'p': serializeItem<Archive, dafBase::Persistable::Ptr>(ar, name, ps); break;
        default:
                  throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, 
                      std::string("Unknown type reading PropertySet") +
                      type + ", name = " + name);
        }
        execTrace("PropertySetFormatter processed " + name + "(" + type = ")");
    }
    execTrace("PropertySetFormatter delegateSerialize end");
}

template void dafPersist::PropertySetFormatter::delegateSerialize(
    boost::archive::binary_oarchive&, unsigned int const version, dafBase::Persistable*);
template void dafPersist::PropertySetFormatter::delegateSerialize(
    boost::archive::binary_iarchive&, unsigned int const version, dafBase::Persistable*);

template
void lsst::daf::persistence::delegateSerialize<lsst::daf::persistence::PropertySetFormatter, boost::mpi::packed_oarchive>(
        boost::mpi::packed_oarchive& ar,
        unsigned int const version,
        lsst::daf::base::Persistable* persistable);

template
void lsst::daf::persistence::delegateSerialize<lsst::daf::persistence::PropertySetFormatter, boost::mpi::packed_iarchive>(
        boost::mpi::packed_iarchive& ar,
        unsigned int const version,
        lsst::daf::base::Persistable* persistable);


/** Factory method for PropertySetFormatter.
 * \param[in] policy Policy for configuring the PropertySetFormatter
 * \return Shared pointer to a new instance
 */
dafPersist::Formatter::Ptr dafPersist::PropertySetFormatter::createInstance(
    pexPolicy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new dafPersist::PropertySetFormatter(policy));
}
