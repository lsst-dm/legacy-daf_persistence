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
 
#ifndef LSST_MWI_PERSISTENCE_PERSISTENCE_H
#define LSST_MWI_PERSISTENCE_PERSISTENCE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for Persistence class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::Persistence
  * @brief Class implementing object persistence.
  *
  * This class persists and retrieves objects by calling Formatter subclasses
  * with a sequence of Storage subclasses that have been configured with
  * LogicalLocations.  This class handles all transaction semantics by starting
  * per-Storage transactions, detecting failures, and causing the Storage
  * subclasses to roll back if necessary.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <vector>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/base/Persistable.h"
#include "lsst/pex/policy.h"
#include "lsst/daf/persistence/Storage.h"

namespace lsst {
namespace daf {
namespace persistence {

// Forward declaration
class LogicalLocation;

class Persistence : public lsst::daf::base::Citizen {
public:
    typedef boost::shared_ptr<Persistence> Ptr;

    virtual ~Persistence(void);

    virtual Storage::Ptr getPersistStorage(std::string const& storageType,
                                           LogicalLocation const& location);
    virtual Storage::Ptr getRetrieveStorage(std::string const& storageType,
                                            LogicalLocation const& location);
    virtual void persist(
        lsst::daf::base::Persistable const& persistable, Storage::List const& storageList,
        lsst::daf::base::PropertySet::Ptr additionalData);
    virtual lsst::daf::base::Persistable::Ptr retrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::daf::base::PropertySet::Ptr additionalData);
    virtual lsst::daf::base::Persistable* unsafeRetrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::daf::base::PropertySet::Ptr additionalData);

    static Ptr getPersistence(lsst::pex::policy::Policy::Ptr policy);

private:
    explicit Persistence(lsst::pex::policy::Policy::Ptr policy);

    // Do not copy or assign Persistence objects
    Persistence(Persistence const&);
    Persistence& operator=(Persistence const&);

    Storage::Ptr _getStorage(std::string const& storageType,
                             LogicalLocation const& location,
                             bool persist);

    lsst::pex::policy::Policy::Ptr _policy;
        ///< Pointer to Policy used to configure Persistence.
};

}}} // namespace lsst::daf::persistence

#endif
