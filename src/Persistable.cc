// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of Persistable base class
 *
 * \author $Author$
 * \version $Revision$
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

#include "lsst/mwi/persistence/Persistable.h"

#include "lsst/mwi/persistence/Persistence.h"

namespace lsst {
namespace mwi {

/// Namespace for persistence subcomponent of mwi component
namespace persistence {

/** Default constructor
 */
Persistable::Persistable(void) {
}

/** Destructor
 */
Persistable::~Persistable(void) {
}


}}} // namespace lsst::mwi::persistence
