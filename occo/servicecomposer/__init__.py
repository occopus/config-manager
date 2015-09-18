#
# Copyright (C) MTA SZTAKI 2014
#

""" Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__  = [ 'ServiceComposer' ]

import occo.util.factory as factory
import occo.util as util
import occo.infobroker as ib
import logging

log = logging.getLogger('occo.servicecomposer')

@ib.provider
class ServiceComposer(factory.MultiBackend, ib.InfoProvider):
    """Abstract interface of a service composer.

    .. todo:: Service Composer documentation.
    """

    @ib.provides('node.service.state')
    def service_status(self, instance_data):
        return self.get_node_state(instance_data)

    def get_node_state(self, instance_data):
        raise NotImplementedError()

import threading

import uuid
def uid():
    return str(uuid.uuid4())

@factory.register(ServiceComposer, 'dummy')
class DummyServiceComposer(ServiceComposer):
    def __init__(self, name='dummy'):
        self.name = name
        self.environments = dict()
        self.node_lookup = dict()
        import occo.infobroker as ib
        self.ib = ib.main_info_broker
        self.lock = threading.RLock()

    @util.wet_method()
    def register_node(self, resolved_node_definition):
        log.debug("[%s] Registering node: %r", 
                self.name, resolved_node_definition['name'])
        with self.lock:
            infra_id = resolved_node_definition['infra_id']

            # Implicitly create an environment for individual nodes.
            # (May not be useful for real SCs!)
            if not infra_id in self.environments:
                log.debug(
                    '[%s] Implicitly creating an environment %r for node %r',
                    self.name, infra_id, resolved_node_definition['name'])
                self.create_infrastructure(infra_id)
            
            env = self.environments[infra_id].setdefault(
                    resolved_node_definition['name'], list())
            env.append(resolved_node_definition)
            self.node_lookup[resolved_node_definition['node_id']] = resolved_node_definition
            log.debug("[%s] Done - '%r'", self.name, self)

    @util.wet_method()
    def drop_node(self, instance_data):
        node_id = instance_data['instance_id']
        if not node_id in self.node_lookup:
            log.debug('[%s] drop_node: Node does not exist; skipping.',
                        self.name)
            return

        log.debug("[%s] Dropping node %r", self.name, node_id)
        with self.lock:
            node = self.node_lookup[node_id]
            infra_id = node.infra_id
            self.environments[infra_id] = list(
                i for i in self.environments[infra_id]
                if i.id != node_id)
            del self.node_lookup[node_id]
            log.debug("[%s] Done - '%r'", self.name, self)

    @util.wet_method()
    def create_infrastructure(self, infra_id):
        log.debug("[%s] Creating infrastructure %r", self.name, infra_id)
        with self.lock:
            self.environments.setdefault(infra_id, dict())
            log.debug("[%s] Done - '%r'", self.name, self)

    @util.wet_method()
    def drop_infrastructure(self, infra_id):
        if not infra_id in self.environments:
            log.debug('[%s] drop_infrastructure: Infrastructure does not exist; skipping.', self.name)
            return
        log.debug("[%s] Dropping infrastructure %r", self.name, infra_id)
        with self.lock:
            del self.environments[infra_id]
            log.debug("[%s] Done - '%r'", self.name, self)

    @util.wet_method('ready')
    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[%s] Querying node state for '%r'", self.name, node_id)
        with self.lock:
            node = self.node_lookup.get(node_id, None)
            state = 'ready' if node else 'unknown'
            log.debug("[%s] Done - %r", self.name, state)
        return state

    @util.wet_method('dummy-value')
    def get_node_attribute(self, node_id, attribute):
        attrspec = attribute \
            if hasattr(attribute, '__iter__') \
            else attribute.split('.')
        return '{{{{{0}{1}}}}}'.format(node_id,
                               ''.join('[{0}]'.format(i) for i in attrspec))

    @util.wet_method(True)
    def infrastructure_exists(self, infra_id):
        return infra_id in self.environments

    def __repr__(self):
        nodelist_repr = lambda nodelist: ', '.join(repr(n) for n in nodelist)
        envlist_repr = list(
            '{0}:[{1}]'.format(k, nodelist_repr(v))
            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)
