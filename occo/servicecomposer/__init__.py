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

import threading

import uuid
def uid():
    return str(uuid.uuid4())

@factory.register(ServiceComposer, 'dummy')
class DummyServiceComposer(ServiceComposer):
    def __init__(self):
        self.environments = dict()
        self.node_lookup = dict()
        import occo.infobroker as ib
        self.ib = ib.main_info_broker
        self.lock = threading.RLock()
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node['name'])
        with self.lock:
            envid = node['environment_id']
            self.environments[envid].setdefault(node['name'], list()).append(node)
            self.node_lookup[node['id']] = node
            log.debug("[SC] Done - '%r'", self)
    def drop_node(self, instance_data):
        node_id = instance_data['instance_id']
        log.debug("[SC] Dropping node '%s'", node_id)
        with self.lock:
            node = self.node_lookup[node_id]
            env_id = node.environment_id
            self.environments[env_id] = list(
                i for i in self.environments[env_id]
                if i.id != node_id)
            del self.node_lookup[node_id]
            log.debug("[SC] Done - '%r'", self)

    def create_environment(self, environment_id):
        log.debug("[SC] Creating environment '%s'", environment_id)
        with self.lock:
            self.environments.setdefault(environment_id, dict())
            log.debug("[SC] Done - '%r'", self)
    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)
        with self.lock:
            del self.environments[environment_id]
            log.debug("[SC] Done - '%r'", self)
    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[SC] Querying node state for '%s'", node_id)
        with self.lock:
            node = self.node_lookup.get(node_id, None)
            state = 'ready' if node else 'unknown'
            log.debug("[SC] Done - '%s'", state)
        return state
    def __repr__(self):
        log.info('%r', self.environments)
        nodelist_repr = lambda nodelist: ', '.join(repr(n) for n in nodelist)
        envlist_repr = list(
            '%s:[%s]'%(k, nodelist_repr(v))
            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)
