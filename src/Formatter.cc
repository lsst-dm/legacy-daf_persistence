// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of Formatter abstract base class
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/Formatter.h"

#include "lsst/mwi/persistence/FormatterRegistry.h"

namespace lsst {
namespace mwi {
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
    lsst::mwi::data::Citizen(type) {
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
    std::string const& name, lsst::mwi::policy::Policy::Ptr policy) {
    return FormatterRegistry::getInstance().lookupFormatter(name, policy);
}

/** Lookup Formatter subclass by its type_info from typeid().
 * \param[in] type std::type_info of Formatter subclass from typeid()
 * \param[in] policy Policy for configuring the Formatter
 * \return Shared pointer to Formatter instance
 */
Formatter::Ptr Formatter::lookupFormatter(
    std::type_info const& type, lsst::mwi::policy::Policy::Ptr policy) {
    return FormatterRegistry::getInstance().lookupFormatter(type, policy);
}

}}} // namespace lsst::mwi::persistence
