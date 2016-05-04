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
 
#ifndef LSST_MWI_PERSISTENCE_BOOSTSTORAGE_H
#define LSST_MWI_PERSISTENCE_BOOSTSTORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for BoostStorage class
  *
  * @author Kian-Tat Lim, SLAC
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::BoostStorage
  * @brief Class for boost::serialization storage.
  *
  * Uses boost::serialization to persist to files.
  *
  * @ingroup daf_persistence
  */


#include "lsst/daf/persistence/Storage.h"

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <memory>
#include <fstream>

namespace lsst {
namespace daf {
namespace persistence {

class BoostStorage : public Storage {
public:
    typedef std::shared_ptr<BoostStorage> Ptr;

    BoostStorage(void);
    virtual ~BoostStorage(void);

    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual boost::archive::text_oarchive& getOArchive(void);
    virtual boost::archive::text_iarchive& getIArchive(void);

private:
    std::unique_ptr<std::ofstream> _ostream; ///< Output stream.
    std::unique_ptr<std::ifstream> _istream; ///< Input stream.
    std::unique_ptr<boost::archive::text_oarchive> _oarchive;
        ///< boost::serialization archive wrapper for output stream.
    std::unique_ptr<boost::archive::text_iarchive> _iarchive;
        ///< boost::serialization archive wrapper for input stream.
};

}}} // lsst::daf::persistence


#endif
