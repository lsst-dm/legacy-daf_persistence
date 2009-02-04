// -*- lsst-c++ -*-

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
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/DbAuth.h"

#include "boost/scoped_array.hpp"
#include <cstdlib>
#include <fstream>

extern "C" {
    #include <pwd.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <unistd.h>
}

#include "lsst/pex/exceptions.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Name of environment variable containing authenticator.
 */
static char const* const envVarName = "LSST_DB_AUTH";

/** Set the authenticator pathname via Policy.
 * \param[in] policy Pointer to a Policy
 */
void DbAuth::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
    if (policy->exists("DbAuthPath")) {
        pathName() = policy->getString("DbAuthPath");
    }
}

/** Determine whether an authenticator string is available for database
 * access.
 * \return True if authenticator is available
 */
bool DbAuth::available(void) {
    try {
        std::string const& auth = DbAuth::authString();
        if (auth.empty()) return false;
    }
    catch (...) {
        return false;
    }
    return true;
}

/** Get the authenticator string for a database.
 * \return String with username:password
 */
std::string const& DbAuth::authString(void) {
    static std::string auth;
    if (auth.empty()) {
        char buffer[256];
        char* authenticator = buffer;
        std::ifstream istr(pathName().c_str());
        if (!istr.fail()) {
            istr.getline(buffer, sizeof(buffer));
            istr.close();
        }
        else {
            authenticator = getenv(envVarName);
            if (authenticator == 0) {
                throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException,
                                  "No database authenticator found");
            }
        }
        auth = std::string(authenticator);
    }
    return auth;
}

/** Get the username to use to authenticate to a database.
 * \return Username string
 */
std::string DbAuth::username(void) {
    std::string const& authenticator(DbAuth::authString());
    size_t pos = authenticator.find(':');
    if (pos != std::string::npos) {
        return authenticator.substr(0, pos);
    }
    else {
        return authenticator;
    }
}

/** Get the password to use to authenticate to a database.
 * \return Password string
 */
std::string DbAuth::password(void) {
    std::string const& authenticator(DbAuth::authString());
    size_t pos = authenticator.find(':');
    if (pos != std::string::npos) {
        return authenticator.substr(pos + 1);
    }
    else {
        return std::string();
    }
}

/** Get a reference to the pathname for the authenticator file.
 * \return Reference to the pathname string
 */
std::string& DbAuth::pathName(void) {
    static std::string path("/nosuchfile");
    static bool homeDirChecked = false;
    if (!homeDirChecked) {
        passwd pwd;
        passwd *pw;
        long maxbuf = sysconf(_SC_GETPW_R_SIZE_MAX);
        boost::scoped_array<char> buffer(new char[maxbuf]);
        int ret = getpwuid_r(geteuid(), &pwd, buffer.get(), maxbuf, &pw);
        if (ret == 0 && pw->pw_dir != 0) {
            std::string filename = std::string(pw->pw_dir) + "/.lsst.db.auth";
            struct stat st;
            ret = stat(filename.c_str(), &st);
            if (ret == 0 && (st.st_mode & (S_IRWXG | S_IRWXO)) == 0) {
                path = filename;
            }
        }
    }
    return path;
}


}}} // lsst::daf::persistence
