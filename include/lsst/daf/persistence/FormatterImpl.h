// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_FORMATTERIMPL_H
#define LSST_MWI_PERSISTENCE_FORMATTERIMPL_H

/** @file
 * @ingroup daf_persistence
 *
 * @brief Auxiliary global template function for Formatter subclasses.
 *
 * This should be included by all Formatter subclass implementations.
 *
 * @author Kian-Tat Lim (ktl@slac.stanford.edu)
 * @version $Revision: 2448 $
 * @date $Date$
 */

namespace lsst {
namespace daf {
namespace persistence {

class Persistable;

/** Template function that serializes a Persistable using boost::serialization.
  * @param[in,out] ar Reference to a boost::archive.
  * @param[in] version Version of the Persistable class.
  * @param[in,out] persistable Pointer to the Persistable instance.
  *
  * @ingroup daf_persistence
  */
template <class FormatterType, class Archive>
inline void delegateSerialize(Archive& ar, unsigned int const version,
                              Persistable* persistable) {
    FormatterType::delegateSerialize(ar, version, persistable);
}

}}} // namespace lsst::daf::persistence

#endif
