// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of Storage abstract base class
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/Storage.h"

#include <cerrno>
#include <sys/stat.h>
#include <unistd.h>

#include "lsst/mwi/exceptions.h"
#include "lsst/mwi/persistence/StorageRegistry.h"

namespace lsst {
namespace mwi {
namespace persistence {

/** Constructor.
 * \param[in] type typeid() of subclass
 */
Storage::Storage(std::type_info const& type) : lsst::mwi::data::Citizen(type) {
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
    lsst::mwi::policy::Policy::Ptr policy) {
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
            lsst::mwi::exceptions::ExceptionData exdata("data");
            exdata <<
                lsst::mwi::data::SupportFactory::createLeafProperty("errno",
                                                                    errno);
            throw lsst::mwi::exceptions::Runtime(exdata,
                "Error creating directory: " + dirName +
                " = " + strerror(errno));
        }
    }
    else if (ret == -1) {
        // We couldn't read the (existing) directory for some reason.
        lsst::mwi::exceptions::ExceptionData exdata("data");
        exdata << lsst::mwi::data::SupportFactory::createLeafProperty("errno",
                                                                      errno);
        throw lsst::mwi::exceptions::Runtime(exdata,
            "Error searching for directory: " + dirName +
            " = " + strerror(errno));
    }
    else if (!S_ISDIR(buf.st_mode)) {
        // It's not a directory.
        throw lsst::mwi::exceptions::Runtime(
            "Non-directory in path: " + dirName);
    }
}

}}} // namespace lsst::mwi::persistence
