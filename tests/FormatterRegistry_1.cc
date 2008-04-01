/**
 * \file FormatterRegistry_1.cc
 *
 * This test tests the FormatterRegistry class.
 */
#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>
#include "lsst/daf/persistence/FormatterRegistry.h"
#include "lsst/pex/exceptions.h"

using namespace lsst::daf::persistence;

#define Assert(b, m) tattle(b, m, __LINE__)

static void tattle(bool mustBeTrue, std::string const& failureMsg, int line) {
    if (! mustBeTrue) {
        std::ostringstream msg;
        msg << __FILE__ << ':' << line << ":\n" << failureMsg << std::ends;
        throw std::runtime_error(msg.str());
    }
}

// A (very) minimal Persistable.
class MyPersistable : public Persistable {
};

// A minimal Formatter.
class MyFormatter : public Formatter {
public:
    MyFormatter(void) : Formatter(typeid(*this)) { };
    // Normally, the following functions would do something.  For testing,
    // they do nothing.
    virtual void write(Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { };
    virtual Persistable* read(Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { return 0; };
    virtual void update(Persistable* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { };
private:
    static Formatter::Ptr createInstance(lsst::pex::policy::Policy::Ptr policy);
    static FormatterRegistration registration;
};

// Register the formatter factory function.
FormatterRegistration MyFormatter::registration("MyPersistable", typeid(MyPersistable), createInstance);

Formatter::Ptr MyFormatter::createInstance(lsst::pex::policy::Policy::Ptr policy) {
    return Formatter::Ptr(new MyFormatter);
}

// Another minimal Formatter, this time without registration and an external
// factory function.  This is not the normal way of writing Formatters; it is
// here for test purposes only.
class YourFormatter : public Formatter {
public:
    YourFormatter(void) : Formatter(typeid(*this)) { };
    virtual void write(Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { };
    virtual Persistable* read(Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { return 0; };
    virtual void update(Persistable* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) { };
};

// External factory function for YourFormatters.  This would normally be a
// static member function as for MyFormatter above.
static Formatter::Ptr factory(lsst::pex::policy::Policy::Ptr policy) {
    return Formatter::Ptr(new YourFormatter);
}

int main(void) {
    std::cout << "Initial setup" << std::endl;
    FormatterRegistry& f(FormatterRegistry::getInstance());
    Formatter::FactoryPtr p = factory;
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    // These tests are to ensure that the basic functionality of the
    // FormatterRegistry is working.  They do not represent the normal method
    // of using the registry and are here for test purposes only.  In
    // particular, registering a Formatter for a built-in type does not make
    // any sense (but cannot be prevented without a lot of extra work).
    std::cout << "Testing explicit registration" << std::endl;
    f.registerFormatter("YourPersistable", typeid(int), p);
    Formatter::Ptr fp = Formatter::lookupFormatter(typeid(int), policy);
    Assert(typeid(*fp) == typeid(YourFormatter), "Didn't get YourFormatter");
    Formatter::Ptr fp2 = Formatter::lookupFormatter("YourPersistable", policy);
    Assert(typeid(*fp2) == typeid(YourFormatter), "Didn't get YourFormatter");
    Assert(fp != fp2, "Old YourFormatter returned");

    // This is the normal way of using FormatterRegistry (i.e. implicitly
    // through Formatter and static FormatterRegistration members).
    std::cout << "Testing static registration" << std::endl;
    Formatter::Ptr fp3 = Formatter::lookupFormatter("MyPersistable", policy);
    Assert(typeid(*fp3) == typeid(MyFormatter), "Didn't get MyFormatter");
    Assert(fp != fp3 && fp2 != fp3, "Old MyFormatter returned");
    Formatter::Ptr fp4 = Formatter::lookupFormatter(typeid(MyPersistable), policy);
    Assert(typeid(*fp4) == typeid(MyFormatter), "Didn't get MyFormatter");
    Assert(fp != fp4 && fp2 != fp4 && fp3 != fp4, "Old MyFormatter returned");

    // These tests look at failure cases, where we try to find a Formatter
    // that doesn't exist.  This should cause an InvalidParameter exception to
    // be thrown.
    std::cout << "Testing unregistered Formatter" << std::endl;
    try {
        Formatter::Ptr fp5 = Formatter::lookupFormatter("FooBar", policy);
        Assert(!fp5, "Got an invalid Formatter for FooBar");
    }
    catch (lsst::pex::exceptions::InvalidParameter) {
        std::cout << "Caught proper exception" << std::endl;
    }
    try {
        Formatter::Ptr fp5 = Formatter::lookupFormatter(typeid(double), policy);
        Assert(!fp5, "Got an invalid Formatter for double");
    }
    catch (lsst::pex::exceptions::InvalidParameter) {
        std::cout << "Caught proper exception" << std::endl;
    }

    return 0;
}
