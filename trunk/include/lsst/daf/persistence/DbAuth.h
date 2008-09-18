// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_DBAUTH_H
#define LSST_MWI_PERSISTENCE_DBAUTH_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for DbAuth class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2673 $
  * @date $Date$
  */

/** @class lsst::daf::persistence::DbAuth
  * @brief Class for database authentication.
  *
  * Provides access to username and password to be used to authenticate to a
  * database.  Actual username and password come from a well-known environment
  * variable or a well-known file or a file specified by Policy.  The format
  * for the authenticator string in any location is "username:password".
  *
  * @ingroup daf_persistence
  */


#include <string>

#include "lsst/pex/policy/Policy.h"

namespace lsst {
namespace daf {
namespace persistence {

class DbAuth {
public:
    static void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    static bool available(void);
    static std::string const& authString(void);
    static std::string username(void);
    static std::string password(void);
private:
    static std::string& pathName(void);
};

}}} // namespace lsst::daf::persistence

#endif
