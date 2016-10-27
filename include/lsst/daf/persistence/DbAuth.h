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

#include "lsst/pex/policy.h"

namespace lsst {
namespace daf {
namespace persistence {

class DbAuth {
public:
    static void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    static bool available(std::string const& host, std::string const& port);
    static std::string authString(std::string const& host,
                                  std::string const& port);
    static std::string username(std::string const& host,
                                std::string const& port);
    static std::string password(std::string const& host,
                                std::string const& port);
};

}}} // namespace lsst::daf::persistence

#endif
