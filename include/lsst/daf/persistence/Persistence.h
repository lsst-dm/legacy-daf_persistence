// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_PERSISTENCE_H
#define LSST_MWI_PERSISTENCE_PERSISTENCE_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for Persistence class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::mwi::persistence::Persistence
  * @brief Class implementing object persistence.
  *
  * This class persists and retrieves objects by calling Formatter subclasses
  * with a sequence of Storage subclasses that have been configured with
  * LogicalLocations.  This class handles all transaction semantics by starting
  * per-Storage transactions, detecting failures, and causing the Storage
  * subclasses to roll back if necessary.
  *
  * @ingroup mwi
  */

#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <vector>

#include "lsst/mwi/data/Citizen.h"
#include "lsst/mwi/data/DataProperty.h"
#include "lsst/mwi/policy/Policy.h"
#include "lsst/mwi/persistence/Persistable.h"
#include "lsst/mwi/persistence/Storage.h"

namespace lsst {
namespace mwi {
namespace persistence {

// Forward declaration
class LogicalLocation;

class Persistence : public lsst::mwi::data::Citizen {
public:
    typedef boost::shared_ptr<Persistence> Ptr;

    virtual ~Persistence(void);

    virtual Storage::Ptr getPersistStorage(std::string const& storageType,
                                           LogicalLocation const& location);
    virtual Storage::Ptr getRetrieveStorage(std::string const& storageType,
                                            LogicalLocation const& location);
    virtual void persist(
        Persistable const& persistable, Storage::List const& storageList,
        lsst::mwi::data::DataProperty::PtrType additionalData);
    virtual Persistable::Ptr retrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::mwi::data::DataProperty::PtrType additionalData);
    virtual Persistable* unsafeRetrieve(
        std::string const& persistableType, Storage::List const& storageList,
        lsst::mwi::data::DataProperty::PtrType additionalData);

    static Ptr getPersistence(lsst::mwi::policy::Policy::Ptr policy);

private:
    explicit Persistence(lsst::mwi::policy::Policy::Ptr policy);

    // Do not copy or assign Persistence objects
    Persistence(Persistence const&);
    Persistence& operator=(Persistence const&);

    Storage::Ptr _getStorage(std::string const& storageType,
                             LogicalLocation const& location,
                             bool persist);

    lsst::mwi::policy::Policy::Ptr _policy;
        ///< Pointer to Policy used to configure Persistence.
};

}}} // namespace lsst::mwi::persistence

#endif
