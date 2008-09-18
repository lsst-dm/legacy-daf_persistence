// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_STORAGEREGISTRY_H
#define LSST_MWI_PERSISTENCE_STORAGEREGISTRY_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for StorageRegistry class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2190 $
  * @date $Date$
  */

/** @class lsst::daf::persistence::StorageRegistry
  * @brief Class to register Storage subclasses.
  *
  * A registry so that subclasses can be looked up by name.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <string>

// #include "lsst/daf/base/Citizen.h"
#include "lsst/daf/persistence/Storage.h"

namespace lsst {
namespace daf {
namespace persistence {

class StorageRegistry {
    // : private lsst::daf::base::Citizen
public:
    Storage::Ptr createInstance(std::string const& name);

    static StorageRegistry& getRegistry(void);

private:
    StorageRegistry(void);
    ~StorageRegistry(void);

    // Do not copy or assign a StorageRegistry.
    StorageRegistry(StorageRegistry const&);
    StorageRegistry& operator=(StorageRegistry const&);
};

}}} // lsst::daf::persistence


#endif
