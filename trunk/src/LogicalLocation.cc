// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of LogicalLocation class.
 *
 * \author $Author: ktlim $
 * \version $Revision: 2286 $
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

#include "lsst/daf/persistence/LogicalLocation.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Default constructor.
 */
LogicalLocation::LogicalLocation(void) :
    lsst::daf::base::Citizen(typeid(*this)) {
}

/** Constructor from string.
 */
LogicalLocation::LogicalLocation(std::string const& locString) :
    lsst::daf::base::Citizen(typeid(*this)), _locString(locString) {
}

/** Accessor.
 */
std::string const& LogicalLocation::locString(void) const {
    return _locString;
}

}}} // namespace lsst::daf::persistence
