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
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Persisting non-PropertySet");
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
                throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
                    std::string("Unknown type ") + type.name() +
                    " in PropertySetFormatter write");
            }
        }
        db->insertRow();
        execTrace("PropertySetFormatter write end");
        return;
    }

    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unrecognized Storage for PropertySet");
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
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unrecognized Storage for PropertySet");
}

void dafPersist::PropertySetFormatter::update(dafBase::Persistable* persistable,
                                   dafPersist::Storage::Ptr storage,
                                   dafBase::PropertySet::Ptr additionalData) {
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unexpected call to update for PropertySet");
}

/** Factory method for PropertySetFormatter.
 * \param[in] policy Policy for configuring the PropertySetFormatter
 * \return Shared pointer to a new instance
 */
dafPersist::Formatter::Ptr dafPersist::PropertySetFormatter::createInstance(
    pexPolicy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new dafPersist::PropertySetFormatter(policy));
}
