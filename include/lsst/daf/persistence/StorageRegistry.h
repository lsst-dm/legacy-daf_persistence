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

#include <memory>
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
