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
 * \brief Implementation of Formatter abstract base class
 *
 * \author $Author$
 * \version $Revision$
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

#include "lsst/daf/persistence/Formatter.h"

#include "lsst/daf/persistence/FormatterRegistry.h"

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.  Registers Formatter subclass factory function.
 * \param[in] persistableName Name of the Persistable subclass
 * \param[in] persistableType typeid() of the Persistable subclass
 * \param[in] factory Factory function for the Formatter, taking a Policy
 */
FormatterRegistration::FormatterRegistration(
    std::string const& persistableName,
    std::type_info const& persistableType,
    Formatter::FactoryPtr factory) {
    FormatterRegistry::getInstance().registerFormatter(persistableName,
                                                       persistableType,
                                                       factory);
}

/** Constructor.
 * \param[in] type typeid() of subclass
 */
Formatter::Formatter(std::type_info const& type) :
    lsst::daf::base::Citizen(type) {
}

/** Minimal destructor.
 */
Formatter::~Formatter(void) {
}

/** Lookup Formatter subclass by name of Persistable subclass.
 * \param[in] name Name of Persistable subclass
 * \param[in] policy Policy for configuring the Formatter
 * \return Shared pointer to Formatter instance
 *
 * Returned pointer is not owned and should not be deleted.
 */
Formatter::Ptr Formatter::lookupFormatter(
    std::string const& name, lsst::pex::policy::Policy::Ptr policy) {
    return FormatterRegistry::getInstance().lookupFormatter(name, policy);
}

/** Lookup Formatter subclass by its type_info from typeid().
 * \param[in] type std::type_info of Formatter subclass from typeid()
 * \param[in] policy Policy for configuring the Formatter
 * \return Shared pointer to Formatter instance
 */
Formatter::Ptr Formatter::lookupFormatter(
    std::type_info const& type, lsst::pex::policy::Policy::Ptr policy) {
    return FormatterRegistry::getInstance().lookupFormatter(type, policy);
}

}}} // namespace lsst::daf::persistence
