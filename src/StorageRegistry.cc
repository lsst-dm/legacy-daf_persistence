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
 * \brief Implementation of StorageRegistry class
 *
 * \author $Author: ktlim $
 * \version $Revision: 2190 $
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

#include "lsst/daf/persistence/StorageRegistry.h"

// Using LSST exceptions has problems due to the use of DataProperty, which is
// a Persistable, in them.
#include <stdexcept>

// All StorageFormatter subclasses must be included here.

#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/DbTsvStorage.h"
#include "lsst/daf/persistence/FitsStorage.h"
#include "lsst/daf/persistence/XmlStorage.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 */
StorageRegistry::StorageRegistry(void) {
    // : lsst::daf::base::Citizen(typeid(this))
}

/** Minimal destructor.  Do not destroy the Storage subclasses in case they
 * are needed at static destruction time.
  */
StorageRegistry::~StorageRegistry(void) {
}

/** Create a StorageFormatter subclass instance by name.
 * \param[in] name Name of subclass
 * \return Shared pointer to subclass instance
 *
 * All StorageFormatter subclasses must be listed here.
 * Implemented as code; could be a lookup in a data structure.
 */
StorageFormatter::Ptr StorageRegistry::createInstance(std::string const& name) {
    if (name == "BoostStorage") {
        return StorageFormatter::Ptr(new BoostStorage);
    }
    else if (name == "DbStorage") {
        return StorageFormatter::Ptr(new DbStorage);
    }
    else if (name == "DbTsvStorage") {
        return StorageFormatter::Ptr(new DbTsvStorage);
    }
    else if (name == "FitsStorage") {
        return StorageFormatter::Ptr(new FitsStorage);
    }
    else if (name == "XmlStorage") {
        return StorageFormatter::Ptr(new XmlStorage);
    }
    else throw std::invalid_argument("Invalid StorageFormatter type: " + name);
}

/** Return a reference to a subclass registry.
 * \return Reference to the registry.
 *
 * Used to guarantee initialization of the registry before use.
 */
StorageRegistry& StorageRegistry::getRegistry(void) {
    static StorageRegistry* registry = new StorageRegistry;
    return *registry;
}

}}} // lsst::daf::persistence
