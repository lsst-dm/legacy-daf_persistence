// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of StorageRegistry class
 *
 * \author $Author: ktlim $
 * \version $Revision: 2190 $
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

#include "lsst/daf/persistence/StorageRegistry.h"

// Using LSST exceptions has problems due to the use of DataProperty, which is
// a Persistable, in them.
#include <stdexcept>

// All Storage subclasses must be included here.

#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/DbTsvStorage.h"
#include "lsst/daf/persistence/FitsStorage.h"
#include "lsst/daf/persistence/XmlStorage.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 */
StorageRegistry::StorageRegistry(void) {
    // : lsst::daf::base::Citizen(typeid(this))
}

/** Minimal destructor.  Do not destroy the Storage subclasses in case they
 * are needed at static destruction time.
  */
StorageRegistry::~StorageRegistry(void) {
}

/** Create a Storage subclass instance by name.
 * \param[in] name Name of subclass
 * \return Shared pointer to subclass instance
 *
 * All Storage subclasses must be listed here.
 * Implemented as code; could be a lookup in a data structure.
 */
Storage::Ptr StorageRegistry::createInstance(std::string const& name) {
    if (name == "BoostStorage") {
        return Storage::Ptr(new BoostStorage);
    }
    else if (name == "DbStorage") {
        return Storage::Ptr(new DbStorage);
    }
    else if (name == "DbTsvStorage") {
        return Storage::Ptr(new DbTsvStorage);
    }
    else if (name == "FitsStorage") {
        return Storage::Ptr(new FitsStorage);
    }
    else if (name == "XmlStorage") {
        return Storage::Ptr(new XmlStorage);
    }
    else throw std::invalid_argument("Invalid storage type: " + name);
}

/** Return a reference to a subclass registry.
 * \return Reference to the registry.
 *
 * Used to guarantee initialization of the registry before use.
 */
StorageRegistry& StorageRegistry::getRegistry(void) {
    static StorageRegistry* registry = new StorageRegistry;
    return *registry;
}

}}} // lsst::daf::persistence
