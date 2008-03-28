// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_BOOSTSTORAGE_H
#define LSST_MWI_PERSISTENCE_BOOSTSTORAGE_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for BoostStorage class
  *
  * @author Kian-Tat Lim, SLAC
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::mwi::persistence::BoostStorage
  * @brief Class for boost::serialization storage.
  *
  * Uses boost::serialization to persist to files.
  *
  * @ingroup mwi
  */


#include "lsst/mwi/persistence/Storage.h"

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/scoped_ptr.hpp>
#include <fstream>

namespace lsst {
namespace mwi {
namespace persistence {

class BoostStorage : public Storage {
public:
    typedef boost::shared_ptr<BoostStorage> Ptr;

    BoostStorage(void);
    virtual ~BoostStorage(void);

    virtual void setPolicy(lsst::mwi::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual boost::archive::text_oarchive& getOArchive(void);
    virtual boost::archive::text_iarchive& getIArchive(void);

private:
    boost::scoped_ptr<std::ofstream> _ostream; ///< Output stream.
    boost::scoped_ptr<std::ifstream> _istream; ///< Input stream.
    boost::scoped_ptr<boost::archive::text_oarchive> _oarchive;
        ///< boost::serialization archive wrapper for output stream.
    boost::scoped_ptr<boost::archive::text_iarchive> _iarchive;
        ///< boost::serialization archive wrapper for input stream.
};

}}} // lsst::mwi::persistence


#endif
