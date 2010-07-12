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
namespace base {

class Persistable;

} // namespace lsst::daf::base

namespace persistence {

/** Template function that serializes a Persistable using boost::serialization.
  * @param[in,out] ar Reference to a boost::archive.
  * @param[in] version Version of the Persistable class.
  * @param[in,out] persistable Pointer to the Persistable instance.
  *
  * @ingroup daf_persistence
  */
template <class FormatterType, class Archive>
inline void delegateSerialize(Archive& ar, unsigned int const version,
                              lsst::daf::base::Persistable* persistable) {
    FormatterType::delegateSerialize(ar, version, persistable);
}

}}} // namespace lsst::daf::persistence

#endif
