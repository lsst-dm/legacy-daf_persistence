// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_FORMATTERREGISTRY_H
#define LSST_MWI_PERSISTENCE_FORMATTERREGISTRY_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for FormatterRegistry class.
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2264 $
  * @date $Date$
  */

/** @class lsst::mwi::persistence::FormatterRegistry
  * @brief Class that registers all Formatter subclasses.
  *
  * Allows lookup by Persistable type_info or name.
  *
  * @ingroup mwi
  */

#include "lsst/mwi/data/Citizen.h"
#include "lsst/mwi/persistence/Formatter.h"

namespace lsst {
namespace mwi {
namespace persistence {

class FormatterRegistry : public lsst::mwi::data::Citizen {
public:
    void registerFormatter(std::string const& persistableName,
                           std::type_info const& persistableType,
                           Formatter::FactoryPtr factory);
    Formatter::Ptr lookupFormatter(std::type_info const& persistableType,
                                   lsst::mwi::policy::Policy::Ptr policy);
    Formatter::Ptr lookupFormatter(std::string const& persistableName,
                                   lsst::mwi::policy::Policy::Ptr policy);

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

}}} // namespace lsst::mwi::persistence

#endif
