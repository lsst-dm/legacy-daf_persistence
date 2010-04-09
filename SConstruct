# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re
import lsst.SConsUtils as scons

dependencies = "boost python mysqlclient utils daf_base pex_logging pex_exceptions pex_policy mpich2".split()
#
# mpich2 1.0.5 (at least) sometimes requires the "pmpich" library.  We could look at the mpicc script
# to check, but this is OK too.  Let's hope that 1.0.8 doesn't have this problem
#
pmpichLib = []
if os.uname()[0] == 'Darwin':
    pmpichLib += [["mpich2", "mpi.h", "pmpich:C++"],]

env = scons.makeEnv("daf_persistence",
                    r"$HeadURL$",
                    [
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
                     ["boost", "boost/version.hpp", "boost_system:C++"],
                     ] + pmpichLib + [
                     ["mpich2", "mpi.h", "mpich:C++"],
                     ["mpich2", "mpi.h", "lmpe:C++"],
                     ["boost", "boost/mpi.hpp", "boost_mpi:C++"],
                     ["python", "Python.h"],
                     ["mysqlclient", "mysql/mysql.h", "mysqlclient_r:C"],
                     ["utils", "lsst/tr1/unordered_map.h", "utils:C++"],
                     ["pex_exceptions", "lsst/pex/exceptions.h", "pex_exceptions:C++"],
                     ["daf_base", "lsst/daf/base.h", "daf_base:C++"],
                     ["pex_logging", "lsst/pex/logging/Trace.h", "pex_logging:C++"],
                     ["pex_policy", "lsst/pex/policy/Policy.h", "pex_policy:C++"]
                    ])
env.Help("""
LSST Data Access Framework persistence package
""")

###############################################################################
# Boilerplate below here

pkg = env["eups_product"]
env.libs[pkg] += env.getlibs(" ".join(dependencies))

#
# Build/install things
#
for d in Split("lib python/lsst/" + re.sub(r'_', "/", pkg) + " examples tests doc"):
    if os.path.isdir(d):
        SConscript(os.path.join(d, "SConscript"))

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "include"),
                  env.Install(env['prefix'], "lib"),
                  env.Install(env['prefix'], "policy"),
                  env.InstallAs(os.path.join(env['prefix'], "doc", "doxygen"),
                                os.path.join("doc", "htmlDir")),
                  env.InstallEups(env['prefix'] + "/ups", glob.glob("ups/*.table"))])

scons.CleanTree(r"*~ core *.so *.os *.o")

#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

env.Declare()
