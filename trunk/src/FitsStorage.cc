// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of FitsStorage class
 *
 * \author $Author$
 * \version $Revision$
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

#include "lsst/daf/persistence/FitsStorage.h"

#include <fstream>

#include "lsst/daf/persistence/LogicalLocation.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 */
FitsStorage::FitsStorage(void) : Storage(typeid(*this)) {
}

/** Destructor.
 */
FitsStorage::~FitsStorage(void) {
}

/** Allow a Policy to be used to configure the FitsStorage.
 * \param[in] policy
 */
void FitsStorage::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
}

/** Set the destination of the FITS file for persistence.
 * \param[in] location Pathname to write to.
 */
void FitsStorage::setPersistLocation(LogicalLocation const& location) {
    _path = location.locString();
    _hdu = 0;
    verifyPathName(_path);
}

/** Set the source of the FITS file for retrieval.
 * \param[in] location Pathname to read from, optionally followed by '#' and
 * HDU number.
 */
void FitsStorage::setRetrieveLocation(LogicalLocation const& location) {
    _path = location.locString();
    size_t loc = _path.find_last_of('#');
    if (loc == std::string::npos) {
        _hdu = 0;
    }
    else {
        _hdu = strtol(_path.substr(loc + 1).c_str(), 0, 10);
        _path = _path.substr(0, loc);
    }
}

/** Start a transaction.
 * No transaction support for now.
 */
void FitsStorage::startTransaction(void) {
}

/** End a transaction.
 * No transaction support for now.
 */
void FitsStorage::endTransaction(void) {
}

/** Return the pathname for the FITS file.
 * \return Pathname
 */
std::string const& FitsStorage::getPath(void) {
    return _path;
}

/** Return the HDU to read from the FITS file.
 * \return Number of the HDU, PDU = 0
 */
int FitsStorage::getHdu(void) {
    return _hdu;
}

}}} // namespace lsst::daf::persistence
