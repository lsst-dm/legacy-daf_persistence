// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_XMLSTORAGE_H
#define LSST_MWI_PERSISTENCE_XMLSTORAGE_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for XmlStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2190 $
  * @date $Date$
  */

/** @class lsst::mwi::persistence::XmlStorage
  * @brief Class for XML file storage.
  *
  * Provides Boost XML archives for Formatter subclasses to use.
  *
  * @ingroup mwi
  */

#include "lsst/mwi/persistence/Storage.h"

#include <boost/archive/xml_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/scoped_ptr.hpp>
#include <fstream>

namespace lsst {
namespace mwi {
namespace persistence {

class XmlStorage : public Storage {
public:
    typedef boost::shared_ptr<XmlStorage> Ptr;

    XmlStorage(void);
    virtual ~XmlStorage(void);

    virtual void setPolicy(lsst::mwi::policy::Policy::Ptr policy);
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

}}} // lsst::mwi::persistence


#endif
