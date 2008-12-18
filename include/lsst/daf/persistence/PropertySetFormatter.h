// -*- lsst-c++ -*-
#ifndef LSST_DAF_PERSISTENCE_PROPERTYSETFORMATTER_H
#define LSST_DAF_PERSISTENCE_PROPERTYSETFORMATTER_H

/** @file
 * @brief Interface for PropertySetFormatter class
 *
 * @version $Revision: 2150 $
 * @date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 * @ingroup daf_persistence
 */

/** @class lsst::daf::persistence::PropertySetFormatter
 * @brief Formatter for persistence of PropertySet instances.
 *
 * @ingroup daf_persistence
 */

#include <lsst/daf/base/PropertySet.h>
#include <lsst/daf/base/Persistable.h>
#include <lsst/daf/persistence/Formatter.h>
#include <lsst/daf/persistence/Storage.h>
#include <lsst/pex/policy/Policy.h>

namespace lsst {
namespace daf {
namespace persistence {

namespace dafBase = lsst::daf::base;
namespace pexPolicy = lsst::pex::policy;

class PropertySetFormatter : public Formatter {
public:
    virtual ~PropertySetFormatter(void);

    virtual void write(dafBase::Persistable const* persistable,
        Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData);

    virtual dafBase::Persistable* read(Storage::Ptr storage,
        dafBase::PropertySet::Ptr additionalData);

    virtual void update(lsst::daf::base::Persistable* persistable,
        Storage::Ptr storage,
        lsst::daf::base::PropertySet::Ptr additionalData);

    template <class Archive>
    static void delegateSerialize(Archive& ar, unsigned int const version,
        dafBase::Persistable* persistable);

private:
    PropertySetFormatter(pexPolicy::Policy::Ptr policy);

    pexPolicy::Policy::Ptr _policy;

    static Formatter::Ptr createInstance(pexPolicy::Policy::Ptr policy);

    static FormatterRegistration registration;
};

}}} // namespace lsst::daf::persistence

#endif
