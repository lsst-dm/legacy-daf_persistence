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
 * \brief Implementation of FormatterRegistry class.
 *
 * \author $Author: ktlim $
 * \version $Revision: 2233 $
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

#include "lsst/daf/persistence/FormatterRegistry.h"

#include "lsst/pex/exceptions.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Get a reference to the singleton instance of the FormatterRegistry.
 * \return Reference to the singleton
 *
 * Uses function-scoped static to ensure proper initialization.
 */
FormatterRegistry& FormatterRegistry::getInstance(void) {
    static FormatterRegistry* registry = new FormatterRegistry;
    return *registry;
}

/** Register a factory for a Formatter subclass using the name and type of the
 * Persistable subclass it formats.
 * \param[in] persistableName Name of the Persistable subclass
 * \param[in] persistableType typeid() of the Persistable subclass
 * \param[in] factory Factory function for the Formatter, taking a Policy
 */
void FormatterRegistry::registerFormatter(
    std::string const& persistableName, std::type_info const& persistableType,
    Formatter::FactoryPtr factory) {
    _byName.insert(FactoryMap::value_type(persistableName, factory));
    _nameForType.insert(StringMap::value_type(persistableType.name(),
                                              persistableName));
}

/** Create a new instance of a Formatter subclass given the typeid() of its
 * corresponding Persistable subclass.
 * \param[in] persistableType typeid() of the Persistable subclass
 * \param[in] policy Policy containing all Formatter policies
 * \return Shared pointer to an instance of the subclass
 */
Formatter::Ptr FormatterRegistry::lookupFormatter(
    std::type_info const& persistableType,
    lsst::pex::policy::Policy::Ptr policy) {
    StringMap::const_iterator it = _nameForType.find(persistableType.name());
    if (it == _nameForType.end()) {
        throw LSST_EXCEPT(lsst::pex::exceptions::InvalidParameterException,
            std::string("No Formatter registered for Persistable type: ") +
            persistableType.name());
    }
    return lookupFormatter(it->second, policy);
}

/** Create a new instance of a Formatter subclass given the string name of its
 * corresponding Persistable subclass.
 * \param[in] persistableName Name of the Persistable subclass
 * \param[in] policy Policy containing all Formatter policies
 * \return Shared pointer to an instance of the subclass
 */
Formatter::Ptr FormatterRegistry::lookupFormatter(
    std::string const& persistableName,
    lsst::pex::policy::Policy::Ptr policy) {
    FactoryMap::const_iterator it = _byName.find(persistableName);
    if (it == _byName.end()) {
        throw LSST_EXCEPT(lsst::pex::exceptions::InvalidParameterException,
            "No Formatter registered for Persistable name: " +
            persistableName);
    }
    lsst::pex::policy::Policy::Ptr formatterPolicy;
    if (policy && policy->exists(persistableName)) {
        formatterPolicy = policy->getPolicy(persistableName);
    }
    return (*(it->second))(formatterPolicy);
}

/** Default constructor.
 */
FormatterRegistry::FormatterRegistry(void) :
    lsst::daf::base::Citizen(typeid(*this)) {
    markPersistent();
}

/** Minimal destructor.
 */
FormatterRegistry::~FormatterRegistry(void) {
}

}}} // namespace lsst::daf::persistence
