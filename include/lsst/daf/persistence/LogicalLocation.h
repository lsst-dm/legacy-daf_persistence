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

#ifndef LSST_MWI_PERSISTENCE_LOGICALLOCATION_H
#define LSST_MWI_PERSISTENCE_LOGICALLOCATION_H

/** @file
 * @ingroup daf_persistence
 *
 * @brief Interface for LogicalLocation class
 *
 * @author Kian-Tat Lim (ktl@slac.stanford.edu)
 * @version $Revision$
 * @date $Date$
 */

/** @class lsst::daf::persistence::LogicalLocation
 * @brief Class for logical location of a persisted Persistable instance.
 *
 * Implemented as a minimal string representing a pathname or a database
 * connection string.  Interpreted by FormatterStorage subclasses.
 *
 * @ingroup daf_persistence
 */

#include <memory>
#include <string>

#include "lsst/base.h"
#include "lsst/daf/base/PropertySet.h"

namespace lsst {
namespace daf {
namespace persistence {

namespace dafBase = lsst::daf::base;

class LogicalLocation {
public:
    typedef std::shared_ptr<LogicalLocation> Ptr;

    LogicalLocation(std::string const& locString,
                    CONST_PTR(dafBase::PropertySet) additionalData = CONST_PTR(dafBase::PropertySet)());
    std::string const& locString(void) const;

    static void setLocationMap(PTR(dafBase::PropertySet) map);

private:
    std::string _locString;                 ///< The location string.
    static PTR(dafBase::PropertySet) _map;  ///< The logical-to-less-logical map.
};

}  // namespace persistence
}  // namespace daf
}  // namespace lsst

#endif
