// -*- lsst-c++ -*-
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

#include "lsst/daf/persistence/Storage.h"

namespace lsst {
namespace daf {
namespace persistence {

class FitsStorage : public Storage {
public:
    typedef boost::shared_ptr<FitsStorage> Ptr;

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
