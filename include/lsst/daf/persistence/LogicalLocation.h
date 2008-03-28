// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_LOGICALLOCATION_H
#define LSST_MWI_PERSISTENCE_LOGICALLOCATION_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for LogicalLocation class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::mwi::persistence::LogicalLocation
  * @brief Class for logical location of a persisted Persistable instance.
  *
  * Implemented as a minimal string representing a pathname or a database
  * connection string.  Interpreted by Storage subclasses.
  *
  * @ingroup mwi
  */

#include <boost/shared_ptr.hpp>
#include <string>

#include "lsst/mwi/data/Citizen.h"

namespace lsst {
namespace mwi {
namespace persistence {

class LogicalLocation : public lsst::mwi::data::Citizen {
public:
    typedef boost::shared_ptr<LogicalLocation> Ptr;

    LogicalLocation(void);
    explicit LogicalLocation(std::string const& locString);
    std::string const& locString(void) const;

private:
    std::string _locString; ///< The location string.
};

}}} // namespace lsst::mwi::persistence

#endif
