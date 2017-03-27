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

#ifndef LSST_MWI_PERSISTENCE_STORAGE_H
#define LSST_MWI_PERSISTENCE_STORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for StorageFormatter abstract base class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::StorageFormatter
  * @brief Abstract base class for StorageFormatter implementations.
  *
  * All subclasses of this base class must be added to StorageRegistry.
  *
  * @ingroup daf_persistence
  */

#include <memory>
#include <map>
#include <string>
#include <typeinfo>

#include "lsst/daf/base/Citizen.h"
#include "lsst/pex/policy.h"

namespace lsst {
namespace daf {
namespace persistence {

class LogicalLocation;

class StorageFormatter : public lsst::daf::base::Citizen {
public:
    typedef std::shared_ptr<StorageFormatter> Ptr;
    typedef std::vector<Ptr> List;

    virtual ~StorageFormatter(void);

    /** Allow a Policy to be used to configure the StorageFormatter.
      * @param[in] policy
      *
      * Should be called first, after construction.
      */
    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy) = 0;

    /** Set the destination for persistence.
      * @param[in] location Location to persist to.
      *
      * Exclusive with setRetrieveLocation().
      */
    virtual void setPersistLocation(LogicalLocation const& location) = 0;

    /** Set the source for retrieval.
      * @param[in] location Location to retrieve from.
      *
      * Exclusive with setPersistLocation().
      */
    virtual void setRetrieveLocation(LogicalLocation const& location) = 0;

    /** Begin an atomic transaction
      */
    virtual void startTransaction(void) = 0;
    /** End an atomic transaction
      */
    virtual void endTransaction(void) = 0;

    static Ptr createInstance(std::string const& name,
                              LogicalLocation const& location,
                              bool persist,
                              lsst::pex::policy::Policy::Ptr policy);

protected:
    explicit StorageFormatter(std::type_info const& type);

    void verifyPathName(std::string const& pathName);

private:
    // Do not copy or assign a StorageFormatter instance.
    StorageFormatter(StorageFormatter const&);
    StorageFormatter& operator=(StorageFormatter const&);
};

}}} // lsst::daf::persistence


#endif
