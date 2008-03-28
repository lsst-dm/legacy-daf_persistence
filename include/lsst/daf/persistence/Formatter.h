// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_FORMATTER_H
#define LSST_MWI_PERSISTENCE_FORMATTER_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for Formatter abstract base class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::mwi::persistence::Formatter
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
  * @ingroup mwi
  */

#include <boost/shared_ptr.hpp>
#include <string>
#include <typeinfo>

#include "lsst/mwi/data/Citizen.h"
#include "lsst/mwi/data/DataProperty.h"
#include "lsst/mwi/persistence/Storage.h"
#include "lsst/mwi/policy/Policy.h"

namespace lsst {
namespace mwi {
namespace persistence {

// Forward declarations.
class LogicalLocation;
class Persistable;


class Formatter : public lsst::mwi::data::Citizen {
public:
    typedef boost::shared_ptr<Formatter> Ptr;

    /** Pointer to a (static) factory function for a Formatter subclass.
      */
    typedef Ptr (*FactoryPtr)(lsst::mwi::policy::Policy::Ptr);


    virtual ~Formatter(void);

    /** Write a Persistable instance to a Storage instance.
      * @param[in] persistable Pointer to the Persistable instance.
      * @param[in] storage Shared pointer to the Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * place to put the instance into the Storage.
      */
    virtual void write(
        Persistable const* persistable, Storage::Ptr storage,
        lsst::mwi::data::DataProperty::PtrType additionalData) = 0;

    /** Read a Persistable instance from a Storage instance.
      * @param[in] storage Pointer to the Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * instance within the Storage.
      * @return Shared pointer to the new Persistable instance.
      */
    virtual Persistable* read(
        Storage::Ptr storage,
        lsst::mwi::data::DataProperty::PtrType additionalData) = 0;

    /** Update an existing Persistable instance with information from
      * an additional Storage instance.
      * @param[in,out] persistable Pointer to the Persistable instance.
      * @param[in] storage Shared pointer to the additional Storage instance.
      * @param[in] additionalData Additional data used to find the proper
      * instance within the Storage.
      */
    virtual void update(
        Persistable* persistable, Storage::Ptr storage,
        lsst::mwi::data::DataProperty::PtrType additionalData) = 0;

    static Formatter::Ptr lookupFormatter(
        std::string const& persistableType,
        lsst::mwi::policy::Policy::Ptr policy);
    static Formatter::Ptr lookupFormatter(
        std::type_info const& persistableType,
        lsst::mwi::policy::Policy::Ptr policy);

protected:
    explicit Formatter(std::type_info const& type);
};

/** @class FormatterRegistration
  * @brief Construct a static instance of this helper class to register a
  * Formatter subclass in the FormatterRegistry.
  *
  * @ingroup mwi
  */
class FormatterRegistration {
public:
    FormatterRegistration(std::string const& persistableName,
                          std::type_info const& persistableType,
                          Formatter::FactoryPtr factory);
};

}}} // namespace lsst::mwi::persistence

#endif
