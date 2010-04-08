// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of LogicalLocation class.
 *
 * \author $Author: ktlim $
 * \version $Revision: 2286 $
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

#include "lsst/daf/persistence/LogicalLocation.h"

#include "boost/regex.hpp"
#include "lsst/pex/exceptions.h"
#include "lsst/pex/logging/Trace.h"

namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;
namespace pexExcept = lsst::pex::exceptions;
namespace pexLog = lsst::pex::logging;

dafBase::PropertySet::Ptr dafPersist::LogicalLocation::_map;

/** Constructor from string and additional data.
 */
dafPersist::LogicalLocation::LogicalLocation(
    std::string const& locString, boost::shared_ptr<dafBase::PropertySet> additionalData) :
    lsst::daf::base::Citizen(typeid(*this)), _locString() {
    boost::regex expr("(%.*?)\\((\\w+?)\\)");
    boost::sregex_iterator i = make_regex_iterator(locString, expr);
    boost::sregex_iterator last;
    pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                      "Input string: " + locString);
    while (i != boost::sregex_iterator()) {
        last = i;
        if ((*i).prefix().matched) {
            _locString += (*i).prefix().str();
        }
        std::string fmt = (*i).str(1);
        std::string key = (*i).str(2);
        pexLog::TTrace<5>("daf.persistence.LogicalLocation", "Key: " + key);
        if (_map && _map->exists(key)) {
            if (_map->typeOf(key) == typeid(int)) {
                int value = _map->getAsInt(key);
                pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                                  "Map Val: %d", value);
                if (fmt == "%") {
                    _locString += (boost::format("%1%") % value).str();
                }
                else {
                    _locString += (boost::format(fmt) % value).str();
                }
            }
            else {
                std::string value = _map->getAsString(key);
                pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                                  "Map Val: " + value);
                _locString += value;
            }
        }
        else if (additionalData && additionalData->exists(key)) {
            if (additionalData->typeOf(key) == typeid(int)) {
                int value = additionalData->getAsInt(key);
                pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                                  "Map Val: %d", value);
                if (fmt == "%") {
                    _locString += (boost::format("%1%") % value).str();
                }
                else {
                    _locString += (boost::format(fmt) % value).str();
                }
            }
            else {
                std::string value = additionalData->getAsString(key);
                pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                                  "Map Val: " + value);
                _locString += value;
            }
        }
        else {
            throw LSST_EXCEPT(pexExcept::RuntimeErrorException,
                              "Unknown substitution: " + key);
        }
        ++i;
    }
    if (last == boost::sregex_iterator()) {
        _locString = locString;
        pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                          "Copy to: " + _locString);
    }
    else {
        _locString += (*last).suffix().str();
        pexLog::TTrace<5>("daf.persistence.LogicalLocation",
                          "Result: " + _locString);
    }
}

/** Accessor.
 */
std::string const& dafPersist::LogicalLocation::locString(void) const {
    return _locString;
}

/** Set the logical-to-less-logical map.
  */
void dafPersist::LogicalLocation::setLocationMap(boost::shared_ptr<dafBase::PropertySet> map) {
    dafBase::PersistentCitizenScope scope;
    if (map) {
        _map = map->deepCopy();
    } else {
        _map.reset();
    }
}
