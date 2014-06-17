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
 * \brief Implementation of Storage abstract base class
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

#include "lsst/daf/persistence/Storage.h"

#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>

#include "lsst/pex/exceptions.h"
#include "lsst/daf/persistence/StorageRegistry.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 * \param[in] type typeid() of subclass
 */
Storage::Storage(std::type_info const& type) : lsst::daf::base::Citizen(type) {
}

/** Minimal destructor
  */
Storage::~Storage(void) {
}

/** Create and configure a Storage subclass instance.
 * \param[in] name Name of subclass
 * \param[in] location Location to persist to or retrieve from
 * \param[in] persist True if persisting, false if retrieving
 * \param[in] policy Policy used to configure the Storage
 * \return Shared pointer to Storage subclass instance
 */
Storage::Ptr Storage::createInstance(
    std::string const& name, LogicalLocation const& location, bool persist,
    lsst::pex::policy::Policy::Ptr policy) {
    Storage::Ptr storage = StorageRegistry::getRegistry().createInstance(name);
    storage->setPolicy(policy);
    if (persist) {
        storage->setPersistLocation(location);
    }
    else {
        storage->setRetrieveLocation(location);
    }
    return storage;
}

/** Ensure that all directories along a path exist, creating them if
 * necessary.
 * \param[in] name Pathname to file to be created
 */
void Storage::verifyPathName(std::string const& name) {
    // Get the directory by stripping off anything after the last slash.
    std::string::size_type pos = name.find_last_of('/');
    if (pos == std::string::npos) return;
    std::string dirName = name.substr(0, pos);

    // Check to see if the directory exists.
    struct stat buf;
    int ret = ::stat(dirName.c_str(), &buf);

    if (ret == -1 && errno == ENOENT) {
        // It doesn't; check its parent and then create it.
        verifyPathName(dirName);

        ret = ::mkdir(dirName.c_str(), 0777);

        // If it already exists, we're OK; otherwise, throw an exception.
        if (ret == -1 && errno != EEXIST) {
            throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
                dirName + ": Error creating directory = " + std::strerror(errno));
        }
    }
    else if (ret == -1) {
        // We couldn't read the (existing) directory for some reason.
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
            dirName + ": Error searching for directory = " + std::strerror(errno));
    }
    else if (!S_ISDIR(buf.st_mode)) {
        // It's not a directory.
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
            dirName + ": Non-directory in path");
    }
}

}}} // namespace lsst::daf::persistence
