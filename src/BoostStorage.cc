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
 * \brief Implementation of BoostStorage class
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

#include "lsst/daf/persistence/BoostStorage.h"

#include <boost/format.hpp>
#include <fstream>
#include <unistd.h>

#include "lsst/daf/persistence/LogicalLocation.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 */
BoostStorage::BoostStorage(void) : Storage(typeid(*this)),
    _ostream(0), _istream(0), _oarchive(0), _iarchive(0) {
}

/** Destructor.
 */
BoostStorage::~BoostStorage(void) {
}

/** Allow a Policy to be used to configure the BoostStorage.
 * \param[in] policy
 */
void BoostStorage::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
}

/** Set the destination of the serialization file for persistence.
 * \param[in] location Pathname to write to.
 */
void BoostStorage::setPersistLocation(LogicalLocation const& location) {
    verifyPathName(location.locString());
    _ostream.reset(new std::ofstream(location.locString().c_str()));
    _oarchive.reset(new boost::archive::text_oarchive(*_ostream));
}

/** Set the source of the serialization file for retrieval.
 * \param[in] location Pathname to read from.
 */
void BoostStorage::setRetrieveLocation(LogicalLocation const& location) {
    char const* fname = location.locString().c_str();
    if (::access(fname, R_OK | F_OK) != 0) {
        throw LSST_EXCEPT(pexExcept::NotFoundException,
                          (boost::format("Unable to access file: %1%")
                           % fname).str());
    }
    _istream.reset(new std::ifstream(fname));
    _iarchive.reset(new boost::archive::text_iarchive(*_istream));
}

/** Start a transaction.
 * No transaction support for now.
 */
void BoostStorage::startTransaction(void) {
}

/** End a transaction.
 * No transaction support for now.
 */
void BoostStorage::endTransaction(void) {
    _oarchive.reset(0);
    _ostream.reset(0);
    _iarchive.reset(0);
    _istream.reset(0);
}

/** Get a \c boost::serialization archive suitable for output.
 * \return Reference to a text output archive
 */
boost::archive::text_oarchive& BoostStorage::getOArchive(void) {
    return *_oarchive;
}

/** Get a \c boost::serialization archive suitable for input.
 * \return Reference to a text input archive
 */
boost::archive::text_iarchive& BoostStorage::getIArchive(void) {
    return *_iarchive;
}

}}} // namespace lsst::daf::persistence
