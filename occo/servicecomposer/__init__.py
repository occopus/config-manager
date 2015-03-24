#
# Copyright (C) MTA SZTAKI 2014
#

""" Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__  = [ 'ServiceComposer' ]

import occo.util.factory as factory
import logging

log = logging.getLogger('occo.servicecomposer')

class ServiceComposer(factory.MultiBackend):
    """Abstract interface of a service composer.

    .. todo:: Service Composer documentation.
    """

@factory.register(ServiceComposer, 'dummy')
class DummyServiceComposer(object):
    def __init__(self, infobroker):
        self.ib = dict()
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node)
        self.ib.environments[node.environment_id].append(node)
        self.ib.node_lookup[node.id] = node
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_node(self, node_id):
        log.debug("[SC] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        env_id = node.environment_id
        self.ib.environments[env_id] = list(
            i for i in self.ib.environments[env_id]
            if i.id != node_id)
        del self.ib.node_lookup[node_id]
        log.debug("[SC] Done - '%r'", self.ib)

    def create_environment(self, environment_id):
        log.debug("[SC] Creating environment '%s'", environment_id)
        self.ib.environments.setdefault(environment_id, [])
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)
        del self.ib.environments[environment_id]
        log.debug("[SC] Done - '%r'", self.ib)
