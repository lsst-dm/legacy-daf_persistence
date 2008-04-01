// -*- lsst-c++ -*-

// Instantiates a boost::shared_ptr with transparent support for
// dynamic down casts from boost::shared_ptr<lsst::daf::persistence::Persistable>
%define %lsst_persistable_shared_ptr(PtrName, CppType...)

    %delobject CppType::swigConvert;
    %newobject CppType::swigConvert;

    %extend CppType {
        static CppType* swigConvert(lsst::daf::persistence::Persistable* p) {
            if (!p) {
                throw lsst::pex::exceptions::Runtime("Cannot convert null Persistable pointer");
            }
            CppType* ptr = dynamic_cast<CppType*>(p);
            if (!ptr) {
                throw lsst::pex::exceptions::Runtime("Persistable was not of the expected type associated with smart pointer " # PtrName);
            }
            return ptr;
        }
    }

    // Convert boost::shared_ptr<lsst::daf::persistence::Persistable> arguments to boost::shared_ptr<CppType >
    %typemap(in) boost::shared_ptr<CppType > const & (boost::shared_ptr<CppType > temp) {
        void* argp = 0;
        int res = SWIG_ConvertPtr($input, &argp, $descriptor(boost::shared_ptr<lsst::daf::persistence::Persistable> *), 0);
        if (SWIG_IsOK(res)) {
            temp = boost::dynamic_pointer_cast<CppType >(*
                reinterpret_cast<boost::shared_ptr<lsst::daf::persistence::Persistable> *>(argp)
            );
            if (!temp) {
                SWIG_exception_fail(SWIG_TypeError, "bad boost::dynamic_pointer_cast");
            }
            $1 = &temp;
        } else {
            res = SWIG_ConvertPtr($input, &argp, $1_descriptor, 0);
            if (!SWIG_IsOK(res)) {
                SWIG_exception_fail(SWIG_ArgError(res), "argument was not a boost::shared_ptr<T> of the expected type");
            }
            $1 = reinterpret_cast<boost::shared_ptr<CppType > *>(argp);
        }
    }

    // Convert lsst::daf::persistence::Persistable* arguments to CppType*
    %typemap(in) CppType* (CppType* temp) {
        void* argp = 0;
        int res = SWIG_ConvertPtr($input, &argp, $descriptor(lsst::daf::persistence::Persistable*), 0);
        if (SWIG_IsOK(res)) {
            temp = dynamic_cast<CppType*>(reinterpret_cast<lsst::daf::persistence::Persistable*>(argp));
            if (!temp) {
                SWIG_exception_fail(SWIG_TypeError, "bad dynamic_cast from Persistable*");
            }
            $1 = temp;
        } else {
            res = SWIG_ConvertPtr($input, &argp, $1_descriptor, 0);
            if (!SWIG_IsOK(res)) {
                SWIG_exception_fail(SWIG_ArgError(res), "argument was not a T* of the expected type");
            }
            $1 = reinterpret_cast<CppType*>(argp);
        }
    }

    %typemap(in) CppType* DISOWN (CppType* temp) {
        void* argp = 0;
        int res = SWIG_ConvertPtr($input, &argp, $descriptor(lsst::daf::persistence::Persistable*), SWIG_POINTER_DISOWN);
        if (SWIG_IsOK(res)) {
            temp = dynamic_cast<CppType*>(reinterpret_cast<lsst::daf::persistence::Persistable*>(argp));
            if (!temp) {
                SWIG_exception_fail(SWIG_TypeError, "bad dynamic_cast from Persistable*");
            }
            $1 = temp;
        } else {
            res = SWIG_ConvertPtr($input, &argp, $1_descriptor, SWIG_POINTER_DISOWN);
            if (!SWIG_IsOK(res)) {
                SWIG_exception_fail(SWIG_ArgError(res), "argument was not a T* of the expected type");
            }
            $1 = reinterpret_cast<CppType*>(argp);
        }
    }

    // To get SWIG to dispatch boost::shared_ptr<lsst::daf::persistence::Persistable> properly,
    // we have to pretend it has the same type as boost::shared_ptr<CppType >
    %typecheck(SWIG_TYPECHECK_POINTER) boost::shared_ptr<CppType > const & {
        void* ptr = 0;
        if (SWIG_IsOK(SWIG_ConvertPtr($input, &ptr, $descriptor(boost::shared_ptr<lsst::daf::persistence::Persistable> *), 0))) {
            $1 = 1;
        } else if (SWIG_IsOK(SWIG_ConvertPtr($input, &ptr, $1_descriptor, 0))) {
            $1 = 1;
        } else {
            $1 = 0;
        }
    }

    // To get SWIG to dispatch lsst::daf::persistence::Persistable* properly,
    // we have to pretend it has the same type as CppType*
    %typecheck(SWIG_TYPECHECK_POINTER) CppType* {
        void* ptr = 0;
        if (SWIG_IsOK(SWIG_ConvertPtr($input, &ptr, $descriptor(lsst::daf::persistence::Persistable*), 0))) {
            $1 = 1;
        } else if (SWIG_IsOK(SWIG_ConvertPtr($input, &ptr, $1_descriptor, 0))) {
            $1 = 1;
        } else {
            $1 = 0;
        }
    }

    // instantiate the shared pointer class - special handling for
    // shared_ptr down-casts provided by the typemaps above
    %template(PtrName) boost::shared_ptr<CppType >;

    // Note: after invoking the macro, the typemaps will "leak" out and provide
    // transparent dynamic_pointer_cast and dynamic_cast for code other than
    // just the shared_ptr constructor

%enddef
