// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of XmlStorage class
 *
 * \author $Author: ktlim $
 * \version $Revision: 2190 $
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

#include "lsst/daf/persistence/XmlStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"

#include <fstream>

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
 */
XmlStorage::XmlStorage(void) : Storage(typeid(*this)),
    _ostream(0), _istream(0), _oarchive(0), _iarchive(0) {
}

/** Destructor.
 *
 * Clean up streams.
 */
XmlStorage::~XmlStorage(void) {
}

/** Allow a Policy to be used to configure the Storage.
 * \param[in] policy
 */
void XmlStorage::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
}

/** Set the destination to persist to.
 * \param[in] location Pathname to Boost XML file
 */
void XmlStorage::setPersistLocation(LogicalLocation const& location) {
    verifyPathName(location.locString());
    _ostream.reset(new std::ofstream(location.locString().c_str()));
    _oarchive.reset(new boost::archive::xml_oarchive(*_ostream));
}

/** Set the source to retrieve from.
 * \param[in] location Pathname to Boost XML file
 */
void XmlStorage::setRetrieveLocation(LogicalLocation const& location) {
    _istream.reset(new std::ifstream(location.locString().c_str()));
    _iarchive.reset(new boost::archive::xml_iarchive(*_istream));
}

/** Start a transaction.
 * No transaction support for now.
 */
void XmlStorage::startTransaction(void) {
}

/** End a transaction.
 * No transaction support for now, but close streams.
 */
void XmlStorage::endTransaction(void) {
    _oarchive.reset(0);
    _ostream.reset(0);
    _iarchive.reset(0);
    _istream.reset(0);
}

/** Get a \c boost::serialization XML archive suitable for output.
 * \return Reference to an XML output archive
 */
boost::archive::xml_oarchive& XmlStorage::getOArchive(void) {
    return *_oarchive;
}

/** Get a \c boost::serialization XML archive suitable for input.
 * \return Reference to an XML input archive
 */
boost::archive::xml_iarchive& XmlStorage::getIArchive(void) {
    return *_iarchive;
}

}}} // namespace lsst::daf::persistence
