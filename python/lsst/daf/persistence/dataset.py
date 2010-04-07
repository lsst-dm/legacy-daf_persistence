"""
classes for describing datasets.  
"""
from lsst.pex.policy import Policy

class Dataset(object):
    """
    a description of a dataset.  

    This description is characterized by a dataset type name and a 
    set of identifiers.  These attributes are access via public member 
    variables 'type' (a string) and ids (a dictionary), respectively.
    """

    def __init__(self, type, path=None, ids=None, **kw):
        """
        create the dataset
        @param type    the dataset type name
        @param path    a filesystem pathname to the file.  If None, the 
                         path is not known/applicable
        @param ids     a dictionary of identifiers, mapping names to values.
                         the type of the identifier is context specific.
        @param *       additional named parameters are taken as 
                         identifiers to be set with the given values
        """
        self.type = type
        self.path = path

        self.ids = None
        if ids:
            self.ids = dict(ids)
        if kw:
            if self.ids is None:
                self.ids = {}
            for key in kw.keys():
                self.ids[key] = kw[key]

    def toString(self, usePath=True):
        """
        return a string form if this dataset's contents
        @param usePath   if true, the path will be used available
        """
        if usePath and self.path:
            return self.path
        out = self.type
        if self.ids is not None:
            for id in self.ids:
                out += "-%s%s" % (id, self.ids[id])
        return out

    def __str__(self):
        return self.toString()

    def toPolicy(self, policy=None):
        """
        return a policy that describes this dataset.
        @param policy    a policy instance to write into.  If not provided
                           (default) a new one is created.
        @return Policy   the policy containing the description of this dataset.
        """
        if not policy:
            policy = Policy()
        if self.type:  policy.set("type", self.type)

        if self.ids:
            ids = Policy()
            policy.set("ids", ids)
            for id in self.ids.keys():
                ids.set(id, self.ids[id])

        if self.path:  policy.set("path", self.path)

        return policy

    def _policy_(self):
        return self.toPolicy()

    @staticmethod
    def fromPolicy(policy):
        """
        unserialize a dataset description from a policy
        """
        type = ids = path = None

        if policy.exists("type"):  type = policy.getString("type")
        if policy.exists("path"):  path = policy.getString("path")
        if policy.exists("ids"):  
            idp = policy.getPolicy("ids")
            ids = {}
            for name in idp.paramNames():
                ids[name] = idp.get(name)

        return Dataset(type, path, ids)
