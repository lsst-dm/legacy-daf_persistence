// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_STORAGEREGISTRY_H
#define LSST_MWI_PERSISTENCE_STORAGEREGISTRY_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for StorageRegistry class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2190 $
  * @date $Date$
  */

/** @class lsst::mwi::persistence::StorageRegistry
  * @brief Class to register Storage subclasses.
  *
  * A registry so that subclasses can be looked up by name.
  *
  * @ingroup mwi
  */

#include <boost/shared_ptr.hpp>
#include <string>

#include "lsst/mwi/data/Citizen.h"
#include "lsst/mwi/persistence/Storage.h"

namespace lsst {
namespace mwi {
namespace persistence {

class StorageRegistry {
    // : private lsst::mwi::data::Citizen
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

}}} // lsst::mwi::persistence


#endif
