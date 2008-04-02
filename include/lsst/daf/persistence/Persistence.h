// -*- lsst-c++ -*-
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
#include "lsst/daf/base/DataProperty.h"
#include "lsst/daf/base/Persistable.h"
#include "lsst/pex/policy/Policy.h"
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
        lsst::daf::base::DataProperty::PtrType additionalData);
    virtual lsst::daf::base::Persistable::Ptr retrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::daf::base::DataProperty::PtrType additionalData);
    virtual lsst::daf::base::Persistable* unsafeRetrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::daf::base::DataProperty::PtrType additionalData);

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
