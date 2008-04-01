// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_STORAGE_H
#define LSST_MWI_PERSISTENCE_STORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for Storage abstract base class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::Storage
  * @brief Abstract base class for storage implementations.
  *
  * All subclasses of this base class must be added to StorageRegistry.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <typeinfo>

#include "lsst/daf/base/Citizen.h"
#include "lsst/pex/policy/Policy.h"

namespace lsst {
namespace daf {
namespace persistence {

class LogicalLocation;

class Storage : public lsst::daf::base::Citizen {
public:
    typedef boost::shared_ptr<Storage> Ptr;
    typedef std::vector<Ptr> List;

    virtual ~Storage(void);

    /** Allow a Policy to be used to configure the Storage.
      * @param[in] policy
      *
      * Should be called first, after construction.
      */
    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy) = 0;

    /** Set the destination for persistence.
      * @param[in] location Location to persist to.
      *
      * Exclusive with setRetrieveLocation().
      */
    virtual void setPersistLocation(LogicalLocation const& location) = 0;

    /** Set the source for retrieval.
      * @param[in] location Location to retrieve from.
      *
      * Exclusive with setPersistLocation().
      */
    virtual void setRetrieveLocation(LogicalLocation const& location) = 0;

    /** Begin an atomic transaction
      */
    virtual void startTransaction(void) = 0;
    /** End an atomic transaction
      */
    virtual void endTransaction(void) = 0;

    static Ptr createInstance(std::string const& name,
                              LogicalLocation const& location,
                              bool persist,
                              lsst::pex::policy::Policy::Ptr policy);

protected:
    explicit Storage(std::type_info const& type);

    void verifyPathName(std::string const& pathName);

private:
    // Do not copy or assign a Storage instance.
    Storage(Storage const&);
    Storage& operator=(Storage const&);
};

}}} // lsst::daf::persistence


#endif
