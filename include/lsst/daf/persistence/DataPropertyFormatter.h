// -*- lsst-c++ -*-
#ifndef LSST_MWI_DATA_DATAPROPERTYFORMATTER_H
#define LSST_MWI_DATA_DATAPROPERTYFORMATTER_H

/** @file
 * @brief Interface for DataPropertyFormatter class
 *
 * @author $Author: ktlim $
 * @version $Revision: 2150 $
 * @date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 * @ingroup daf
 */

/** @class lsst::daf::persistence::DataPropertyFormatter
 * @brief Formatter for persistence of DataProperty instances.
 *
 * @ingroup daf
 */

#include <lsst/daf/base/DataProperty.h>
#include <lsst/daf/base/Persistable.h>
#include <lsst/daf/persistence/Formatter.h>
#include <lsst/daf/persistence/Storage.h>
#include <lsst/pex/policy/Policy.h>

// Forward declarations of Boost archive types
namespace boost {
namespace archive {
    class xml_oarchive;
    class xml_iarchive;
}} // namespace boost::archive;

namespace lsst {
namespace daf {
namespace persistence {

class DataPropertyFormatter : public lsst::daf::persistence::Formatter {
public:
    virtual ~DataPropertyFormatter(void);

    virtual void write(
        lsst::daf::base::Persistable const* persistable,
        lsst::daf::persistence::Storage::Ptr storage,
        lsst::daf::base::DataProperty::PtrType additionalData
    );

    virtual lsst::daf::base::Persistable* read(
        lsst::daf::persistence::Storage::Ptr storage,
        lsst::daf::base::DataProperty::PtrType additionalData
    );

    virtual void update(
        lsst::daf::base::Persistable* persistable,
        lsst::daf::persistence::Storage::Ptr storage,
        lsst::daf::base::DataProperty::PtrType additionalData
    );

    template <class Archive>
    static void delegateSerialize(
        Archive& ar,
        unsigned int const version,
        lsst::daf::base::Persistable* persistable
    );

private:
    DataPropertyFormatter(lsst::pex::policy::Policy::Ptr policy);

    lsst::pex::policy::Policy::Ptr _policy;

    static lsst::daf::persistence::Formatter::Ptr createInstance(
        lsst::pex::policy::Policy::Ptr policy);

    static lsst::daf::persistence::FormatterRegistration registration;
};

}}} // namespace lsst::daf::persistence

#endif
