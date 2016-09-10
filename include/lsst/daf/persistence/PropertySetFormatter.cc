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
 * \brief Implementation of PropertySetFormatter class
 *
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf_persistence
 */

#include <string>
#include <vector>

#include <boost/serialization/nvp.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include <lsst/daf/base/PropertySet.h>
#include "lsst/daf/base/DateTime.h"
#include <lsst/pex/exceptions.h>


using boost::serialization::make_nvp;

namespace lsst {
namespace daf {
namespace persistence {

namespace dafBase = lsst::daf::base;

namespace {

/** Serialize a PropertySet value.
 * \param[in,out] ar Boost archive
 * \param[in] name Name of property to serialize/deserialize.
 * \param[in,out] ps Pointer to PropertySet.
 */
template <class Archive, typename T>
static void serializeItem(Archive& ar, std::string const& name,
                          dafBase::PropertySet* ps) {
    std::vector<T> value;
    if (Archive::is_saving::value) {
        value = ps->getArray<T>(name);
        ar & make_nvp("value", value);
    }
    else {
        ar & make_nvp("value", value);
        ps->set(name, value);
    }
}

/** Serialize a DateTime PropertySet value.
 * \param[in,out] ar Boost archive
 * \param[in] name Name of property to serialize/deserialize.
 * \param[in,out] ps Pointer to PropertySet.
 */
template <class Archive>
static void serializeDateTime(Archive& ar, std::string const& name,
                              dafBase::PropertySet* ps) {
    std::vector<dafBase::DateTime> value;
    std::vector<long long> nsecs;
    if (Archive::is_saving::value) {
        value = ps->getArray<dafBase::DateTime>(name);
        for (std::vector<dafBase::DateTime>::const_iterator i = value.begin();
             i != value.end(); ++i) {
            nsecs.push_back(i->nsecs());
        }
        ar & make_nvp("value", nsecs);
    }
    else {
        ar & make_nvp("value", nsecs);
        for (std::vector<long long>::const_iterator i = nsecs.begin();
             i != nsecs.end(); ++i) {
            value.push_back(dafBase::DateTime(*i));
        }
        ps->set(name, value);
    }
}

} // anonymous namespace

/** Serialize a PropertySet to a Boost archive.  Handles text or XML
 * archives, input or output.
 * \param[in,out] ar Boost archive
 * \param[in] version Version of the PropertySet class
 * \param[in,out] persistable Pointer to the PropertySet as a Persistable
 */
template <class Archive>
void PropertySetFormatter::delegateSerialize(
    Archive& ar, unsigned int const version, dafBase::Persistable* persistable) {
    dafBase::PropertySet* ps =
        dynamic_cast<dafBase::PropertySet*>(persistable);
    if (ps == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, "Serializing non-PropertySet");
    }
    ar & make_nvp("base",
                  boost::serialization::base_object<dafBase::Persistable>(*ps));

    std::vector<std::string> names;
    size_t nNames;
    if (Archive::is_saving::value) {
        names = ps->paramNames(false);
        nNames = names.size();
    }
    ar & make_nvp("nitems", nNames);

    char type;
    std::string name;
    for (size_t i = 0; i < nNames; ++i) {
        if (Archive::is_saving::value) {
            name = names[i];
            std::type_info const& id(ps->typeOf(name));

            if (id == typeid(bool)) type = 'b';
            else if (id == typeid(char)) type = 'c';
            else if (id == typeid(signed char)) type = 'y';
            else if (id == typeid(unsigned char)) type = 'C';
            else if (id == typeid(short)) type = 'w';
            else if (id == typeid(unsigned short)) type = 'W';
            else if (id == typeid(int)) type = 'i';
            else if (id == typeid(unsigned int)) type = 'I';
            else if (id == typeid(long)) type = 'l';
            else if (id == typeid(unsigned long)) type = 'L';
            else if (id == typeid(long long)) type = 'x';
            else if (id == typeid(unsigned long long)) type = 'X';
            else if (id == typeid(float)) type = 'f';
            else if (id == typeid(double)) type = 'd';
            else if (id == typeid(std::string)) type = 's';
            else if (id == typeid(dafBase::DateTime)) type = 'T';
            else if (id == typeid(dafBase::Persistable::Ptr)) type = 'p';
            else {
                throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
                                  name +
                                  ": Unknown type in PropertySet serialize");
            }
        }

        ar & make_nvp("name", name);
        ar & make_nvp("type", type);
        switch (type) {
        case 'b': serializeItem<Archive, bool>(ar, name, ps); break;
        case 'c': serializeItem<Archive, char>(ar, name, ps); break;
        case 'y': serializeItem<Archive, signed char>(ar, name, ps); break;
        case 'C': serializeItem<Archive, unsigned char>(ar, name, ps); break;
        case 'w': serializeItem<Archive, short>(ar, name, ps); break;
        case 'W': serializeItem<Archive, unsigned short>(ar, name, ps); break;
        case 'i': serializeItem<Archive, int>(ar, name, ps); break;
        case 'I': serializeItem<Archive, unsigned int>(ar, name, ps); break;
        case 'l': serializeItem<Archive, long>(ar, name, ps); break;
        case 'L': serializeItem<Archive, unsigned long>(ar, name, ps); break;
        case 'x': serializeItem<Archive, long long>(ar, name, ps); break;
        case 'X': serializeItem<Archive, unsigned long long>(ar, name, ps); break;
        case 'f': serializeItem<Archive, float>(ar, name, ps); break;
        case 'd': serializeItem<Archive, double>(ar, name, ps); break;
        case 's': serializeItem<Archive, std::string>(ar, name, ps); break;
        case 'T': serializeDateTime<Archive>(ar, name, ps); break;
        case 'p': serializeItem<Archive, dafBase::Persistable::Ptr>(ar, name, ps); break;
        default:
                  throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError,
                      std::string("Unknown type reading PropertySet") +
                      type + ", name = " + name);
        }
    }
}

}}} // namespace lsst::daf::persistence
