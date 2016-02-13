

from lsst.daf.persistence import Mapper, Policy, ButlerLocation

#<?cfg paf policy ?>



class RepositoryMapper(Mapper):

    def __init__(self, cfg):
        # todo I'm guessing the policy would probably want to come from the default in-package location, and
        # then be overridden where desired by policy in repository root, and then
        # have the cfg policy applied
        self.policy = cfg['policy']
        self.access = cfg['access']

    @classmethod
    def cfg(cls, policy=None, access=None):
        return Policy({'cls':cls, 'policy':policy, 'access':access})

    def __repr__(self):
        if 'policy' in self.__dict__ and 'access' in self.__dict__:
            return 'RepositoryMapper(policy=%s, access=%s)' %(self.policy, self.access)
        else:
            return 'uninitialized RepositoryMapper'

    def map_cfg(self, dataId, write):
        """Map a location for a cfg file.

        :param dataId: keys & values to be applied to the template.
        :param write: True if this map is being done do perform a write operation, else assumes read. Will
                      verify location exists if write is True.
        :return: a butlerLocation that describes the mapped location.
        """
        # todo check: do we need keys to complete dataId? (search Registry)
        template = self.policy['repositories.cfg.template']
        location = template % dataId
        if write is False and self.access.storage.exists(location) is False:
            return None
        bl = ButlerLocation(
            pythonType = self.policy['repositories.cfg.python'],
            cppType = None,
            storageName = self.policy['repositories.cfg.storage'],
            locationList = (self.access.storage.locationWithRoot(location),),
            dataId = dataId,
            mapper = self)
        return bl


    def map_repo(self, dataId, write):
        if write is True:
            return None

        # todo check: do we need keys to complete dataId? (search Registry)

        template = self.policy['repositories.repo.template']
        location = template % dataId
        if self.access.storage.exists(location):
            bl = ButlerLocation(
                pythonType = self.policy['repositories.repo.python'],
                cppType = None,
                storageName = None,
                locationList = (location,),
                dataId = dataId,
                mapper = self)
            return bl

        return None

# repoCfg = butler.get('repo', {version:123})
# repoCfg = butler.get('repo', {validRange:('2016-02-09T14:23:44.885709', '2016-02-10T14:23:44.885709')})
