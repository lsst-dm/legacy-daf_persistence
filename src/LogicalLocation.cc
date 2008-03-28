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
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/LogicalLocation.h"

namespace lsst {
namespace mwi {
namespace persistence {

/** Default constructor.
 */
LogicalLocation::LogicalLocation(void) :
    lsst::mwi::data::Citizen(typeid(*this)) {
}

/** Constructor from string.
 */
LogicalLocation::LogicalLocation(std::string const& locString) :
    lsst::mwi::data::Citizen(typeid(*this)), _locString(locString) {
}

/** Accessor.
 */
std::string const& LogicalLocation::locString(void) const {
    return _locString;
}

}}} // namespace lsst::mwi::persistence
