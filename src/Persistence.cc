// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of Persistence class
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/Persistence.h"

#include <boost/regex.hpp>

#include "lsst/mwi/persistence/Formatter.h"
#include "lsst/mwi/persistence/LogicalLocation.h"
#include "lsst/mwi/persistence/Persistable.h"
#include "lsst/mwi/policy/Policy.h"
#include "lsst/mwi/persistence/Storage.h"

namespace lsst {
namespace mwi {
namespace persistence {

/** Constructor.
 * \param[in] policy Policy to configure the Persistence object
 */
Persistence::Persistence(lsst::mwi::policy::Policy::Ptr policy) :
    lsst::mwi::data::Citizen(typeid(*this)), _policy(policy) {
}

/** Destructor.
 */
Persistence::~Persistence(void) {
}

/** Create a Storage subclass configured for a particular access.
 * \param[in] storageType Name of Storage subclass as registered in
 * StorageRegistry
 * \param[in] location Location to persist to or retrieve from
 * (subclass-specific)
 * \param[in] persist True if persisting, false if retrieving
 */
Storage::Ptr Persistence::_getStorage(std::string const& storageType,
                                      LogicalLocation const& location,
                                      bool persist) {
    lsst::mwi::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(storageType)) {
        policyPtr = _policy->getPolicy(storageType);
    }
    return Storage::createInstance(storageType, location, persist, policyPtr);
}

/** Create a Storage subclass configured to persist to a location.
 * \param[in] storageType Name of Storage subclass as registered in
 * StorageRegistry
 * \param[in] location Location to persist to (subclass-specific)
 */
Storage::Ptr Persistence::getPersistStorage(std::string const& storageType,
                                            LogicalLocation const& location) {
    return _getStorage(storageType, location, true);
}

/** Create a Storage subclass configured to retrieve from a location.
 * \param[in] storageType Name of Storage subclass as registered in
 * StorageRegistry
 * \param[in] location Location to retrieve from (subclass-specific)
 */
Storage::Ptr Persistence::getRetrieveStorage(std::string const& storageType,
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
    Persistable const& persistable, Storage::List const& storageList,
    lsst::mwi::data::DataProperty::PtrType additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::mwi::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f =
        Formatter::lookupFormatter(typeid(persistable), policyPtr);
    // Use the Formatter instance to write the Persistable to each Storage
    // in turn.  Commit the transactions (in order) when all writing is
    // complete.
    for (Storage::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
        f->write(&persistable, *it, additionalData);
    }
    /// \todo Add in more transaction handling -- KTL 2007-06-26
    for (Storage::List::const_iterator it = storageList.begin();
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
Persistable* Persistence::unsafeRetrieve(
    std::string const& persistableType, Storage::List const& storageList,
    lsst::mwi::data::DataProperty::PtrType additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::mwi::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f = Formatter::lookupFormatter(persistableType, policyPtr);
    // Use the Formatter instance to read from the first Storage; then update
    // from each additional Storage in turn.
    Persistable* persistable = 0;
    for (Storage::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
        if (!persistable) {
            persistable = f->read(*it, additionalData);
        } else {
            f->update(persistable, *it, additionalData);
        }
    }
    for (Storage::List::const_iterator it = storageList.begin();
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
 * correct data from any of the Storages
 * \return Shared pointer to new Persistable instance
 */
Persistable::Ptr Persistence::retrieve(
    std::string const& persistableType, Storage::List const& storageList,
    lsst::mwi::data::DataProperty::PtrType additionalData) {
    return Persistable::Ptr(
        unsafeRetrieve(persistableType, storageList, additionalData));
}

/** Create a Persistence object.
 * \param[in] policy Policy to configure the Persistence object
 * \return Pointer to a Persistence instance
 */
Persistence::Ptr Persistence::getPersistence(
    lsst::mwi::policy::Policy::Ptr policy) {
    return Persistence::Ptr(new Persistence(policy));
}


}}} // namespace lsst::mwi::persistence
