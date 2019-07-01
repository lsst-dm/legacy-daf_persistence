// -*- lsst-c++ -*-

/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010, 2016 LSST Corporation.
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
 * \brief Implementation of DbAuth class
 *
 * \author $Author: ktlim $
 * \version $Revision: 2673 $
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf_persistence
 */

#ifndef __GNUC__
#define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/DbAuth.h"

#include <cstdlib>
#include <fstream>
#include <memory>

extern "C" {
#include <pwd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
}

#include "lsst/pex/exceptions.h"

namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;
namespace pexPolicy = lsst::pex::policy;

static pexPolicy::Policy::Ptr authPolicy(static_cast<pexPolicy::Policy*>(0));

static std::pair<std::string, std::string> search(std::string const& host, std::string const& port) {
    if (authPolicy == 0) {
        passwd pwd;
        passwd* pw;
        long maxbuf = sysconf(_SC_GETPW_R_SIZE_MAX);
        std::unique_ptr<char[]> buffer(new char[maxbuf]);
        int ret = getpwuid_r(geteuid(), &pwd, buffer.get(), maxbuf, &pw);
        if (ret != 0 || pw->pw_dir == 0) {
            throw LSST_EXCEPT(pexExcept::RuntimeError, "Could not get home directory");
        }
        std::string dir = std::string(pw->pw_dir) + "/.lsst";
        std::string filename = dir + "/db-auth.paf";
        struct stat st;
        ret = stat(dir.c_str(), &st);
        if (ret != 0 || (st.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
            throw LSST_EXCEPT(pexExcept::RuntimeError, dir + " directory is missing or accessible by others");
        }
        ret = stat(filename.c_str(), &st);
        if (ret != 0 || (st.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
            throw LSST_EXCEPT(pexExcept::RuntimeError, filename + " is missing or accessible by others");
        }
        { authPolicy = pexPolicy::Policy::Ptr(new pexPolicy::Policy(filename)); }
    }
    int portNum = atoi(port.c_str());
    pexPolicy::Policy::PolicyPtrArray authArray = authPolicy->getPolicyArray("database.authInfo");
    for (pexPolicy::Policy::PolicyPtrArray::const_iterator i = authArray.begin(); i != authArray.end(); ++i) {
        if ((*i)->getString("host") == host && (*i)->getInt("port") == portNum) {
            std::string username = (*i)->getString("user");
            std::string password = (*i)->getString("password");
            if (username.empty()) {
                throw LSST_EXCEPT(pexExcept::RuntimeError,
                                  "Empty username for host/port: " + host + ":" + port);
            }
            return std::pair<std::string, std::string>(username, password);
        }
    }
    throw LSST_EXCEPT(pexExcept::RuntimeError, "No credentials found for host/port: " + host + ":" + port);
    return std::pair<std::string, std::string>("", "");  // not reached
}

/** Set the authenticator Policy.
 * \param[in] policy Pointer to a Policy
 */
void dafPersist::DbAuth::setPolicy(pexPolicy::Policy::Ptr policy) {
    authPolicy = pexPolicy::Policy::Ptr(new pexPolicy::Policy(*policy, true));
}

/** Set the authenticator Policy back to null.
 */
void dafPersist::DbAuth::resetPolicy() { authPolicy = nullptr; }

/** Determine whether an authenticator string is available for database
 * access.
 * \param[in] host Name of the host to connect to.
 * \param[in] port Port number to connect to (as string).
 * \return True if authenticator is available
 */
bool dafPersist::DbAuth::available(std::string const& host, std::string const& port) {
    try {
        std::pair<std::string, std::string> result = search(host, port);
        return true;
    } catch (...) {
        return false;
    }
    return false;  // not reached
}

/** Get the authenticator string for a database.
 * \param[in] host Name of the host to connect to.
 * \param[in] port Port number to connect to (as string).
 * \return String with username:password
 */
std::string dafPersist::DbAuth::authString(std::string const& host, std::string const& port) {
    std::pair<std::string, std::string> result = search(host, port);
    return result.first + ":" + result.second;
}

/** Get the username to use to authenticate to a database.
 * \param[in] host Name of the host to connect to.
 * \param[in] port Port number to connect to (as string).
 * \return Username string
 */
std::string dafPersist::DbAuth::username(std::string const& host, std::string const& port) {
    std::pair<std::string, std::string> result = search(host, port);
    return result.first;
}

/** Get the password to use to authenticate to a database.
 * \param[in] host Name of the host to connect to.
 * \param[in] port Port number to connect to (as string).
 * \return Password string
 */
std::string dafPersist::DbAuth::password(std::string const& host, std::string const& port) {
    std::pair<std::string, std::string> result = search(host, port);
    return result.second;
}
