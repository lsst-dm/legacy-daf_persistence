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

#ifndef LSST_MWI_PERSISTENCE_FITSSTORAGE_H
#define LSST_MWI_PERSISTENCE_FITSSTORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for FitsStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::FitsStorage
  * @brief Class for FITS file storage.
  *
  * Merely maintains pathname and HDU number for Formatter subclasses to use.
  *
  * @ingroup daf_persistence
  */

#include "lsst/daf/persistence/StorageFormatter.h"

namespace lsst {
namespace daf {
namespace persistence {

class FitsStorage : public StorageFormatter {
public:
    typedef std::shared_ptr<FitsStorage> Ptr;

    FitsStorage(void);
    virtual ~FitsStorage(void);

    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual std::string const& getPath(void);
    virtual int getHdu(void);

private:
    std::string _path;
    int _hdu;
};

}}} // lsst::daf::persistence


#endif
