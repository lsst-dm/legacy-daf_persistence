// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_LOGICALLOCATION_H
#define LSST_MWI_PERSISTENCE_LOGICALLOCATION_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for LogicalLocation class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::LogicalLocation
  * @brief Class for logical location of a persisted Persistable instance.
  *
  * Implemented as a minimal string representing a pathname or a database
  * connection string.  Interpreted by Storage subclasses.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <string>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"

namespace lsst {
namespace daf {
namespace persistence {

namespace dafBase = lsst::daf::base;

class LogicalLocation : public dafBase::Citizen {
public:
    typedef boost::shared_ptr<LogicalLocation> Ptr;

    LogicalLocation(std::string const& locString,
                    boost::shared_ptr<dafBase::PropertySet> additionalData =
                    dafBase::PropertySet::Ptr());
    std::string const& locString(void) const;

    static void setLocationMap(boost::shared_ptr<dafBase::PropertySet> map);

private:
    std::string _locString; ///< The location string.
    static dafBase::PropertySet::Ptr _map; ///< The logical-to-less-logical map.
};

}}} // namespace lsst::daf::persistence

#endif
