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
 
#ifndef LSST_MWI_PERSISTENCE_FORMATTERREGISTRY_H
#define LSST_MWI_PERSISTENCE_FORMATTERREGISTRY_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for FormatterRegistry class.
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2264 $
  * @date $Date$
  */

/** @class lsst::daf::persistence::FormatterRegistry
  * @brief Class that registers all Formatter subclasses.
  *
  * Allows lookup by Persistable type_info or name.
  *
  * @ingroup daf_persistence
  */

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/persistence/Formatter.h"

namespace lsst {
namespace daf {
namespace persistence {

class FormatterRegistry : public lsst::daf::base::Citizen {
public:
    void registerFormatter(std::string const& persistableName,
                           std::type_info const& persistableType,
                           Formatter::FactoryPtr factory);
    Formatter::Ptr lookupFormatter(std::type_info const& persistableType,
                                   lsst::pex::policy::Policy::Ptr policy);
    Formatter::Ptr lookupFormatter(std::string const& persistableName,
                                   lsst::pex::policy::Policy::Ptr policy);

    static FormatterRegistry& getInstance(void);

private:
    typedef std::map<std::string, Formatter::FactoryPtr> FactoryMap;
    typedef std::map<std::string, std::string> StringMap;

    FormatterRegistry(void);
    ~FormatterRegistry(void);

    // Do not copy or assign a FormatterRegistry.
    FormatterRegistry(FormatterRegistry const&);
    FormatterRegistry& operator=(FormatterRegistry const&);

    FactoryMap _byName;
        ///< Registry of Formatter factories by Persistable name.
    StringMap _nameForType;
        ///< Registry of Persistable names by std::type_info::name().
};

}}} // namespace lsst::daf::persistence

#endif
