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
 
#ifndef LSST_MWI_PERSISTENCE_FORMATTER_H
#define LSST_MWI_PERSISTENCE_FORMATTER_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for Formatter abstract base class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::Formatter
  * @brief Abstract base class for all formatters.
  *
  * Formatters map Persistable subclasses into an appropriate form for output
  * to Storage subclasses and vice versa upon retrieval.  They also may use an
  * additional piece of data to select the appropriate data for retrieval.
  *
  * Subclasses of Formatter must register themselves by creating a static
  * instance of the FormatterRegistration class with the name and type_info
  * of the Persistable class they are formatting and a factory method to
  * create instances of the subclass using a Policy.  If they are to be
  * used with boost::serialization, subclasses of Formatter must also
  * implement a public static delegateSerialize() template (or a set of
  * static delegateSerialize() functions for each supported archive type).
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <string>
#include <typeinfo>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/Storage.h"
#include "lsst/pex/policy.h"

namespace lsst {
namespace daf {
namespace base {

class Persistable;

} // namespace lsst::daf::base

namespace persistence {

// Forward declarations.
class LogicalLocation;


class Formatter : public lsst::daf::base::Citizen {
public:
    typedef boost::shared_ptr<Formatter> Ptr;

    /** Pointer to a (static) factory function for a Formatter subclass.
      */
    typedef Ptr (*FactoryPtr)(lsst::pex::policy::Policy::Ptr);


    virtual ~Formatter(void);

    /** Write a Persistable instance to a Storage instance.
      * @param[in] persistable Pointer to the Persistable instance.
      * @param[in] storage Shared pointer to the Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * place to put the instance into the Storage.
      */
    virtual void write(
        lsst::daf::base::Persistable const* persistable, Storage::Ptr storage,
        lsst::daf::base::PropertySet::Ptr additionalData) = 0;

    /** Read a Persistable instance from a Storage instance.
      * @param[in] storage Pointer to the Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * instance within the Storage.
      * @return Shared pointer to the new Persistable instance.
      */
    virtual lsst::daf::base::Persistable* read(
        Storage::Ptr storage,
        lsst::daf::base::PropertySet::Ptr additionalData) = 0;

    /** Update an existing Persistable instance with information from
      * an additional Storage instance.
      * @param[in,out] persistable Pointer to the Persistable instance.
      * @param[in] storage Shared pointer to the additional Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * instance within the Storage.
      */
    virtual void update(
        lsst::daf::base::Persistable* persistable, Storage::Ptr storage,
        lsst::daf::base::PropertySet::Ptr additionalData) = 0;

    static Formatter::Ptr lookupFormatter(
        std::string const& persistableType,
        lsst::pex::policy::Policy::Ptr policy);
    static Formatter::Ptr lookupFormatter(
        std::type_info const& persistableType,
        lsst::pex::policy::Policy::Ptr policy);

protected:
    explicit Formatter(std::type_info const& type);
};

/** @class FormatterRegistration
  * @brief Construct a static instance of this helper class to register a
  * Formatter subclass in the FormatterRegistry.
  *
  * @ingroup daf_persistence
  */
class FormatterRegistration {
public:
    FormatterRegistration(std::string const& persistableName,
                          std::type_info const& persistableType,
                          Formatter::FactoryPtr factory);
};

}}} // namespace lsst::daf::persistence

#endif
