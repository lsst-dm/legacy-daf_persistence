# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, os
import lsst.SConsUtils as scons

env = scons.makeEnv("mwi",
                    r"$HeadURL$",
                    [["boost", "boost/version.hpp", "boost_filesystem:C++"],
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
                     ["python", "Python.h"],
                     ["jaula", "jaula/jaula_parse.h", "jaula:C++"],
                     ["seal",  "SealBase/config.h", "lcg_SealBase lcg_SealKernel lcg_PluginManager:C++" ],
                     ["coral", "RelationalAccess/ConnectionService.h", "lcg_CoralBase lcg_RelationalService:C++"],
                     ["mysqlclient", "mysql/mysql.h", "mysqlclient_r:C"]
                    ])

#
# Is C++'s TR1 available?  If not, use e.g. #include "lsst/tr1/foo.h"
#
# This test is in SConsUtils >= 1.17, so when a suitable version is deployed we can delete the test from here
#
if not re.search(r"LSST_HAVE_TR1", str(env['CCFLAGS'])):
    conf = env.Configure()
    env.Append(CCFLAGS = '-DLSST_HAVE_TR1=%d' % int(conf.CheckHeader("tr1/unordered_map", language="C++")))
    conf.Finish()
#
# Build/install things
#
for d in Split("examples include/lsst/mwi/exceptions lib tests python/lsst/mwi doc"):
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
env.Help("""
LSST FrameWork packages
""")

