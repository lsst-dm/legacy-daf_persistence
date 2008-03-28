// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_DBAUTH_H
#define LSST_MWI_PERSISTENCE_DBAUTH_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for DbAuth class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2673 $
  * @date $Date$
  */

/** @class lsst::mwi::persistence::DbAuth
  * @brief Class for database authentication.
  *
  * Provides access to username and password to be used to authenticate to a
  * database.  Actual username and password come from a well-known environment
  * variable or a well-known file or a file specified by Policy.
  *
  * @ingroup mwi
  */


#include <string>

#include "lsst/mwi/policy/Policy.h"

namespace lsst {
namespace mwi {
namespace persistence {

class DbAuth {
public:
    static void setPolicy(lsst::mwi::policy::Policy::Ptr policy);
    static bool available(void);
    static std::string const& authString(void);
    static std::string username(void);
    static std::string password(void);
private:
    static std::string& pathName(void);
};

}}} // namespace lsst::mwi::persistence

#endif
