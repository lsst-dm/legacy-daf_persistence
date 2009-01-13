# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re
import lsst.SConsUtils as scons

dependencies = "boost python mysqlclient utils daf_base pex_logging pex_exceptions pex_policy mpich".split()

env = scons.makeEnv("daf_persistence",
                    r"$HeadURL$",
                    [
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
                     ["boost", "boost/version.hpp", "boost_system:C++"],
                     ["mpich2", "mpi.h", "mpich:C++"],
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
    SConscript(os.path.join(d, "SConscript"))

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "include"),
                  env.Install(env['prefix'], "lib"),
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
