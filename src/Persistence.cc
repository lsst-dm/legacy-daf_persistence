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
 * \brief Implementation of Persistence class
 *
 * \author $Author$
 * \version $Revision$
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

#include "lsst/daf/persistence/Persistence.h"

#include <boost/regex.hpp>

#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/base/Persistable.h"
#include "lsst/pex/policy/Policy.h"
#include "lsst/daf/persistence/StorageFormatter.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 * \param[in] policy Policy to configure the Persistence object
 */
Persistence::Persistence(lsst::pex::policy::Policy::Ptr policy) :
    lsst::daf::base::Citizen(typeid(*this)), _policy(policy) {
}

/** Destructor.
 */
Persistence::~Persistence(void) {
}

/** Create a StorageFormatter subclass configured for a particular access.
 * \param[in] storageType Name of StorageFormatter subclass as registered in
 * StorageRegistry
 * \param[in] location Location to persist to or retrieve from
 * (subclass-specific)
 * \param[in] persist True if persisting, false if retrieving
 */
StorageFormatter::Ptr Persistence::_getStorage(std::string const& storageType,
                                               LogicalLocation const& location,
                                               bool persist) {
    lsst::pex::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(storageType)) {
        policyPtr = _policy->getPolicy(storageType);
    }
    return StorageFormatter::createInstance(storageType, location, persist, policyPtr);
}

/** Create a StorageFormatter subclass configured to persist to a location.
 * \param[in] storageType Name of StorageFormatter subclass as registered in
 * StorageRegistry
 * \param[in] location Location to persist to (subclass-specific)
 */
StorageFormatter::Ptr Persistence::getPersistStorage(std::string const& storageType,
                                            LogicalLocation const& location) {
    return _getStorage(storageType, location, true);
}

/** Create a StorageFormatter subclass configured to retrieve from a location.
 * \param[in] storageType Name of StorageFormatter subclass as registered in
 * StorageRegistry
 * \param[in] location Location to retrieve from (subclass-specific)
 */
StorageFormatter::Ptr Persistence::getRetrieveStorage(std::string const& storageType,
                                             LogicalLocation const& location) {
    return _getStorage(storageType, location, false);
}

/** Persist a Persistable instance.
 * \param[in] persistable The Persistable instance
 * \param[in] storageList List of storages to persist to (in order)
 * \param[in] additionalData Additional information needed to determine the
 * correct place to put data in any of the Storages
 */
void Persistence::persist(
    lsst::daf::base::Persistable const& persistable, StorageFormatter::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::pex::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f =
        Formatter::lookupFormatter(typeid(persistable), policyPtr);
    // Use the Formatter instance to write the Persistable to each StorageFormatter
    // in turn.  Commit the transactions (in order) when all writing is
    // complete.
    for (StorageFormatter::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
        f->write(&persistable, *it, additionalData);
    }
    for (StorageFormatter::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->endTransaction();
    }
}

/** Retrieve a Persistable instance, returning an unsafe bare pointer.
 * Intended for use by SWIG/Python only.
 * \param[in] persistableType Name of Persistable type to be retrieved as
 * registered by its Formatter
 * \param[in] storageList List of storages to retrieve from (in order)
 * \param[in] additionalData Additional information needed to select the
 * correct data from any of the Storages
 * \return Bare pointer to new Persistable instance
 */
lsst::daf::base::Persistable* Persistence::unsafeRetrieve(
    std::string const& persistableType, StorageFormatter::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::pex::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f = Formatter::lookupFormatter(persistableType, policyPtr);
    // Use the Formatter instance to read from the first StorageFormatter; then update
    // from each additional StorageFormatter in turn.
    lsst::daf::base::Persistable* persistable = 0;
    for (StorageFormatter::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
        if (!persistable) {
            persistable = f->read(*it, additionalData);
        } else {
            f->update(persistable, *it, additionalData);
        }
    }
    for (StorageFormatter::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->endTransaction();
    }
    return persistable;
}

/** Retrieve a Persistable instance.
 * \param[in] persistableType Name of Persistable type to be retrieved as
 * registered by its Formatter
 * \param[in] storageList List of storages to retrieve from (in order)
 * \param[in] additionalData Additional information needed to select the
 * correct data from any of the StorageFormatters
 * \return Shared pointer to new Persistable instance
 */
lsst::daf::base::Persistable::Ptr Persistence::retrieve(
    std::string const& persistableType, StorageFormatter::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData) {
    return lsst::daf::base::Persistable::Ptr(
        unsafeRetrieve(persistableType, storageList, additionalData));
}

/** Create a Persistence object.
 * \param[in] policy Policy to configure the Persistence object
 * \return Pointer to a Persistence instance
 */
Persistence::Ptr Persistence::getPersistence(
    lsst::pex::policy::Policy::Ptr policy) {
    return Persistence::Ptr(new Persistence(policy));
}

/** Return the policy used to configure the Persistence object
 *
 * \return Pointer to Policy
 */
lsst::pex::policy::Policy::Ptr Persistence::getPolicy() const
{
    return _policy;
}


}}} // namespace lsst::daf::persistence
