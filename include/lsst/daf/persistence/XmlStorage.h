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
 
#ifndef LSST_MWI_PERSISTENCE_XMLSTORAGE_H
#define LSST_MWI_PERSISTENCE_XMLSTORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for XmlStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2190 $
  * @date $Date$
  */

/** @class lsst::daf::persistence::XmlStorage
  * @brief Class for XML file storage.
  *
  * Provides Boost XML archives for Formatter subclasses to use.
  *
  * @ingroup daf_persistence
  */

#include "lsst/daf/persistence/Storage.h"

#include <boost/archive/xml_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/scoped_ptr.hpp>
#include <fstream>

namespace lsst {
namespace daf {
namespace persistence {

class XmlStorage : public Storage {
public:
    typedef boost::shared_ptr<XmlStorage> Ptr;

    XmlStorage(void);
    virtual ~XmlStorage(void);

    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual boost::archive::xml_oarchive& getOArchive(void);
    virtual boost::archive::xml_iarchive& getIArchive(void);

private:
    boost::scoped_ptr<std::ofstream> _ostream;
        ///< Underlying output stream.
    boost::scoped_ptr<std::ifstream> _istream;
        ///< Underlying input stream.
    boost::scoped_ptr<boost::archive::xml_oarchive> _oarchive;
        ///< Boost XML output archive.
    boost::scoped_ptr<boost::archive::xml_iarchive> _iarchive;
        ///< Boost XML input archive.
};

}}} // lsst::daf::persistence


#endif
