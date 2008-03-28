// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_FITSSTORAGE_H
#define LSST_MWI_PERSISTENCE_FITSSTORAGE_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for FitsStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::mwi::persistence::FitsStorage
  * @brief Class for FITS file storage.
  *
  * Merely maintains pathname and HDU number for Formatter subclasses to use.
  *
  * @ingroup mwi
  */

#include "lsst/mwi/persistence/Storage.h"

namespace lsst {
namespace mwi {
namespace persistence {

class FitsStorage : public Storage {
public:
    typedef boost::shared_ptr<FitsStorage> Ptr;

    FitsStorage(void);
    virtual ~FitsStorage(void);

    virtual void setPolicy(lsst::mwi::policy::Policy::Ptr policy);
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

}}} // lsst::mwi::persistence


#endif
