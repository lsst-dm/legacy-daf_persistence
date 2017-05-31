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
FitsStorage::FitsStorage(void) : FormatterStorage(typeid(*this)) {
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
    _hdu = INT_MIN;
    verifyPathName(_path);
}

/** Set the source of the FITS file for retrieval.
 * \param[in] location Pathname to read from, optionally followed by bracketed
 * HDU number.
 */
void FitsStorage::setRetrieveLocation(LogicalLocation const& location) {
    _path = location.locString();
    size_t loc = _path.find_last_of('[');
    if (loc == std::string::npos) {
        _hdu = INT_MIN;
    }
    else {
        _hdu = strtol(_path.substr(loc + 1).c_str(), 0, 10);
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
