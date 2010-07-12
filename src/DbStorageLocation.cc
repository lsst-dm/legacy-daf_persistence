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
 * \brief Implementation for DbStorageLocation class
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 * \ingroup daf_persistence
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/DbStorageLocation.h"

#include <boost/regex.hpp>

#include "lsst/pex/exceptions.h"
#include "lsst/daf/persistence/DbAuth.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Default constructor.
 */
DbStorageLocation::DbStorageLocation(void) :
    lsst::daf::base::Citizen(typeid(*this)),
    _dbType(), _hostname(), _port(), _username(), _password(), _dbName() {
}

/** Constructor from CORAL-style URL.
 * \param[in] url CORAL-style connection string (database type, hostname,
 * port, database name)
 */
DbStorageLocation::DbStorageLocation(std::string const& url) :
    lsst::daf::base::Citizen(typeid(*this)) {
    boost::smatch what;
    boost::regex
        expression("(\\w+)://(\\S+):(\\d+)/(\\S+)");
    if (boost::regex_match(url, what, expression)) {
        _dbType = what[1];
        _hostname = what[2];
        _port = what[3];
        _dbName = what[4];
        _username = DbAuth::username(_hostname, _port);
        _password = DbAuth::password(_hostname, _port);
    }
    else {
        throw LSST_EXCEPT(lsst::pex::exceptions::InvalidParameterException,
            "Unparseable connection string passed to DbStorageLocation: " +
            url);
    }
}

/** Destructor.
 */
DbStorageLocation::~DbStorageLocation(void) {
}

/** Produce a string (URL) representation of the DbStorageLocation.
 * \return String suitable for constructing another DbStorageLocation
 */
std::string DbStorageLocation::toString(void) const {
    return _dbType + "://" + _username + ":" + _password + "@" +
        _hostname + ":" + _port + "/" + _dbName;
}

/** Produce a CORAL-style connection string representation of the
 * DbStorageLocation.
 * \return String suitable for passing to CORAL functions
 */
std::string DbStorageLocation::getConnString(void) const {
    return _dbType + "://" + _hostname + ":" + _port + "/" + _dbName;
}

/** Accessor for database type.
 * \return Reference to database type string.
 */
std::string const& DbStorageLocation::getDbType(void) const {
    return _dbType;
}

/** Accessor for database hostname.
 * \return Reference to database hostname string.
 */
std::string const& DbStorageLocation::getHostname(void) const {
    return _hostname;
}

/** Accessor for database port number.
 * \return Reference to database port number string.
 */
std::string const& DbStorageLocation::getPort(void) const {
    return _port;
}

/** Accessor for username.
 * \return Reference to username string.
 */
std::string const& DbStorageLocation::getUsername(void) const {
    return _username;
}

/** Accessor for password.
 * \return Reference to password string.
 */
std::string const& DbStorageLocation::getPassword(void) const {
    return _password;
}

/** Accessor for database name.
 * \return Reference to database name string.
 */
std::string const& DbStorageLocation::getDbName(void) const {
    return _dbName;
}

}}} // namespace lsst::daf::persistence
