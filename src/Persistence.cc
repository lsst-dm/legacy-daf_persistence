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
#include "lsst/daf/persistence/Storage.h"

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
    lsst::pex::policy::Policy::Ptr policyPtr;
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
 * \param[in] iter Number of instance if it is part of a list
 * \param[in] len Length of list this instance is part of
 */
void Persistence::persist(
    lsst::daf::base::Persistable const& persistable, Storage::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData, int iter, int len) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::pex::policy::Policy::Ptr policyPtr;
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
        if (iter == 0) {
            (*it)->startTransaction();
        }
        f->write(&persistable, *it, additionalData, iter, len);
    }
    /// \todo Add in more transaction handling -- KTL 2007-06-26
    if (iter == len - 1) {
        for (Storage::List::const_iterator it = storageList.begin();
             it != storageList.end(); ++it) {
            (*it)->endTransaction();
        }
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
    std::string const& persistableType, Storage::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::pex::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f = Formatter::lookupFormatter(persistableType, policyPtr);
    // Use the Formatter instance to read from the first Storage; then update
    // from each additional Storage in turn.
    lsst::daf::base::Persistable* persistable = 0;
    bool done;
    for (Storage::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
        if (!persistable) {
            persistable = f->read(*it, additionalData, &done);
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

/** Retrieve a vector of Persistable instances.
 * \param[in] persistableType Name of Persistable type to be retrieved as
 * registered by its Formatter
 * \param[in] storageList List of storages to retrieve from (in order)
 * \param[in] additionalData Additional information needed to select the
 * correct data from any of the Storages
 * \return Vector of shared pointers to new Persistable instances
 */
std::vector<boost::shared_ptr<lsst::daf::base::Persistable> >
Persistence::retrieveVector(
    std::string const& persistableType, Storage::List const& storageList,
    lsst::daf::base::PropertySet::Ptr additionalData) {
    // Get the policies for all Formatters, if present
    std::string policyName = "Formatter";
    lsst::pex::policy::Policy::Ptr policyPtr;
    if (_policy && _policy->exists(policyName)) {
        policyPtr = _policy->getPolicy(policyName);
    }
    // Find the appropriate Formatter.
    Formatter::Ptr f = Formatter::lookupFormatter(persistableType, policyPtr);
    // Use the Formatter instance to read from the first Storage; then update
    // from each additional Storage in turn.
    lsst::daf::base::Persistable* persistable;
    for (Storage::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->startTransaction();
    }
    bool done = false;
    std::vector<boost::shared_ptr<lsst::daf::base::Persistable> > result;
    while (!done) {
        persistable = 0;
        try {
            for (Storage::List::const_iterator it = storageList.begin();
                 it != storageList.end(); ++it) {
                if (!persistable) {
                    persistable = f->read(*it, additionalData, &done);
                } else {
                    f->update(persistable, *it, additionalData);
                }
            }
            result.push_back(
                boost::shared_ptr<lsst::daf::base::Persistable>(persistable));
        }
        catch (...) {
            done = true;
        }
    }
    for (Storage::List::const_iterator it = storageList.begin();
         it != storageList.end(); ++it) {
        (*it)->endTransaction();
    }
    return result;
}

/** Retrieve a Persistable instance.
 * \param[in] persistableType Name of Persistable type to be retrieved as
 * registered by its Formatter
 * \param[in] storageList List of storages to retrieve from (in order)
 * \param[in] additionalData Additional information needed to select the
 * correct data from any of the Storages
 * \return Shared pointer to new Persistable instance
 */
lsst::daf::base::Persistable::Ptr Persistence::retrieve(
    std::string const& persistableType, Storage::List const& storageList,
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


}}} // namespace lsst::daf::persistence
