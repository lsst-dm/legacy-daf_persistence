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



/** \file
 * \brief Implementation of PropertySetFormatter class
 *
 * \version $Revision: 2151 $
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf_persistence
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/PropertySetFormatter.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/serialization/nvp.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

#include <lsst/daf/base/PropertySet.h>
#include "lsst/daf/base/DateTime.h"
#include "lsst/daf/persistence/FormatterImpl.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/XmlStorage.h"
#include <lsst/pex/exceptions.h>
#include <lsst/log/Log.h>
#include <lsst/pex/policy/Policy.h>

namespace {
auto _log = LOG_GET("daf.persistence.PropertySetFormatter");
}

namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;
namespace pexPolicy = lsst::pex::policy;

using boost::serialization::make_nvp;

/** Register this Formatter subclass through a static instance of
 * FormatterRegistration.
 */
dafPersist::FormatterRegistration
dafPersist::PropertySetFormatter::registration("PropertySet",
                                               typeid(dafBase::PropertySet),
                                               createInstance);

/** Constructor.
 * \param[in] policy Policy for configuring this Formatter
 */
dafPersist::PropertySetFormatter::PropertySetFormatter(
    pexPolicy::Policy::Ptr policy) :
    dafPersist::Formatter(typeid(*this)), _policy(policy) {
}

/** Minimal destructor.
 */
dafPersist::PropertySetFormatter::~PropertySetFormatter(void) {
}

void dafPersist::PropertySetFormatter::write(
    dafBase::Persistable const* persistable,
    dafPersist::FormatterStorage::Ptr storage,
    dafBase::PropertySet::Ptr additionalData) {
    LOGLS_TRACE(_log, "PropertySetFormatter write start");
    dafBase::PropertySet const* ps =
        dynamic_cast<dafBase::PropertySet const*>(persistable);
    if (ps == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Persisting non-PropertySet");
    }
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        LOGLS_TRACE(_log, "PropertySetFormatter write BoostStorage");
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        boost->getOArchive() & *ps;
        LOGLS_TRACE(_log, "PropertySetFormatter write end");
        return;
    }
    else if (typeid(*storage) == typeid(dafPersist::XmlStorage)) {
        LOGLS_TRACE(_log, "PropertySetFormatter write XmlStorage");
        dafPersist::XmlStorage* xml =
            dynamic_cast<dafPersist::XmlStorage*>(storage.get());
        xml->getOArchive() & make_nvp("propertySet", *ps);
        LOGLS_TRACE(_log, "PropertySetFormatter write end");
        return;
    }

    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unrecognized FormatterStorage for PropertySet");
}

dafBase::Persistable* dafPersist::PropertySetFormatter::read(
    dafPersist::FormatterStorage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    LOGLS_TRACE(_log, "PropertySetFormatter read start");
    dafBase::PropertySet* ps = new dafBase::PropertySet;
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        LOGLS_TRACE(_log, "PropertySetFormatter read BoostStorage");
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        boost->getIArchive() & *ps;
        LOGLS_TRACE(_log, "PropertySetFormatter read end");
        return ps;
    }
    else if (typeid(*storage) == typeid(dafPersist::XmlStorage)) {
        LOGLS_TRACE(_log, "PropertySetFormatter read XmlStorage");
        dafPersist::XmlStorage* xml =
            dynamic_cast<dafPersist::XmlStorage*>(storage.get());
        xml->getIArchive() & make_nvp("propertySet", *ps);
        LOGLS_TRACE(_log, "PropertySetFormatter read end");
        return ps;
    }
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unrecognized FormatterStorage for PropertySet");
}

void dafPersist::PropertySetFormatter::update(dafBase::Persistable* persistable,
                                   dafPersist::FormatterStorage::Ptr storage,
                                   dafBase::PropertySet::Ptr additionalData) {
    throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Unexpected call to update for PropertySet");
}

/** Factory method for PropertySetFormatter.
 * \param[in] policy Policy for configuring the PropertySetFormatter
 * \return Shared pointer to a new instance
 */
dafPersist::Formatter::Ptr dafPersist::PropertySetFormatter::createInstance(
    pexPolicy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new dafPersist::PropertySetFormatter(policy));
}
