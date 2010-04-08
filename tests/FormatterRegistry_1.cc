/**
 * \file FormatterRegistry_1.cc
 *
 * This test tests the FormatterRegistry class.
 */
#include <sstream>
#include <string>
#include "lsst/daf/base/Persistable.h"
#include "lsst/daf/persistence/FormatterRegistry.h"
#include "lsst/pex/exceptions.h"

#define BOOST_TEST_MODULE FormatterRegistry_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;

// A (very) minimal Persistable.
class MyPersistable : public dafBase::Persistable {
};

// A minimal Formatter.
class MyFormatter : public dafPersist::Formatter {
public:
    MyFormatter(void) : dafPersist::Formatter(typeid(*this)) { };
    // Normally, the following functions would do something.  For testing,
    // they do nothing.
    virtual void write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, int iter, int len) { };
    virtual dafBase::Persistable* read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, bool first, bool* done) { *done = true; return 0; };
    virtual void update(dafBase::Persistable* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) { };
private:
    static dafPersist::Formatter::Ptr createInstance(lsst::pex::policy::Policy::Ptr policy);
    static dafPersist::FormatterRegistration registration;
};

// Register the formatter factory function.
dafPersist::FormatterRegistration MyFormatter::registration("MyPersistable", typeid(MyPersistable), createInstance);

dafPersist::Formatter::Ptr MyFormatter::createInstance(lsst::pex::policy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new MyFormatter);
}

// Another minimal Formatter, this time without registration and an external
// factory function.  This is not the normal way of writing Formatters; it is
// here for test purposes only.
class YourFormatter : public dafPersist::Formatter {
public:
    YourFormatter(void) : dafPersist::Formatter(typeid(*this)) { };
    virtual void write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, int iter, int len) { };
    virtual dafBase::Persistable* read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, bool first, bool* done) { *done = true; return 0; };
    virtual void update(dafBase::Persistable* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) { };
};

// External factory function for YourFormatters.  This would normally be a
// static member function as for MyFormatter above.
static dafPersist::Formatter::Ptr factory(lsst::pex::policy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new YourFormatter);
}

BOOST_AUTO_TEST_SUITE(FormatterRegistrySuite)

BOOST_AUTO_TEST_CASE(FormatterRegistry1) {
    dafPersist::FormatterRegistry& f(dafPersist::FormatterRegistry::getInstance());
    dafPersist::Formatter::FactoryPtr p = factory;
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    // These tests are to ensure that the basic functionality of the
    // FormatterRegistry is working.  They do not represent the normal method
    // of using the registry and are here for test purposes only.  In
    // particular, registering a Formatter for a built-in type does not make
    // any sense (but cannot be prevented without a lot of extra work).
    f.registerFormatter("YourPersistable", typeid(int), p);
    dafPersist::Formatter::Ptr fp = dafPersist::Formatter::lookupFormatter(typeid(int), policy);
    BOOST_CHECK_MESSAGE(typeid(*fp) == typeid(YourFormatter), "Didn't get YourFormatter");
    dafPersist::Formatter::Ptr fp2 = dafPersist::Formatter::lookupFormatter("YourPersistable", policy);
    BOOST_CHECK_MESSAGE(typeid(*fp2) == typeid(YourFormatter), "Didn't get YourFormatter");
    BOOST_CHECK_MESSAGE(fp != fp2, "Old YourFormatter returned");

    // This is the normal way of using FormatterRegistry (i.e. implicitly
    // through Formatter and static FormatterRegistration members).
    dafPersist::Formatter::Ptr fp3 = dafPersist::Formatter::lookupFormatter("MyPersistable", policy);
    BOOST_CHECK_MESSAGE(typeid(*fp3) == typeid(MyFormatter), "Didn't get MyFormatter");
    BOOST_CHECK_MESSAGE(fp != fp3 && fp2 != fp3, "Old MyFormatter returned");
    dafPersist::Formatter::Ptr fp4 = dafPersist::Formatter::lookupFormatter(typeid(MyPersistable), policy);
    BOOST_CHECK_MESSAGE(typeid(*fp4) == typeid(MyFormatter), "Didn't get MyFormatter");
    BOOST_CHECK_MESSAGE(fp != fp4 && fp2 != fp4 && fp3 != fp4, "Old MyFormatter returned");

    // These tests look at failure cases, where we try to find a Formatter
    // that doesn't exist.  This should cause an InvalidParameter exception to
    // be thrown.
    BOOST_CHECK_THROW(dafPersist::Formatter::lookupFormatter("FooBar", policy), lsst::pex::exceptions::InvalidParameterException);
    BOOST_CHECK_THROW(dafPersist::Formatter::lookupFormatter(typeid(double), policy), lsst::pex::exceptions::InvalidParameterException);
}

BOOST_AUTO_TEST_SUITE_END()
