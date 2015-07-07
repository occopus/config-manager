#
# Copyright (C) MTA SZTAKI 2014
#

""" Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__  = [ 'ServiceComposer' ]

import occo.util.factory as factory
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
    def __init__(self):
        self.environments = dict()
        self.node_lookup = dict()
        import occo.infobroker as ib
        self.ib = ib.main_info_broker
        self.lock = threading.RLock()

    def register_node(self, resolved_node_definition):
        log.debug("[SC] Registering node: %r", resolved_node_definition['name'])
        with self.lock:
            infra_id = resolved_node_definition['infra_id']

            # Implicitly create an environment for individual nodes.
            # (May not be useful for real SCs!)
            if not infra_id in self.environments:
                log.debug(
                    '[SC] Implicitly creating an environment %r for node %r',
                    infra_id, resolved_node_definition['name'])
                self.create_infrastructure(infra_id)

            env = self.environments[infra_id].setdefault(
                resolved_node_definition['name'], list())
            env.append(resolved_node_definition)
            self.node_lookup[resolved_node_definition['node_id']] = resolved_node_definition
            log.debug("[SC] Done - '%r'", self)

    def drop_node(self, instance_data):
        node_id = instance_data['instance_id']
        if not node_id in self.node_lookup:
            log.debug('[SC] drop_node: Node does not exist; skipping.')
            return

        log.debug("[SC] Dropping node '%s'", node_id)
        with self.lock:
            node = self.node_lookup[node_id]
            infra_id = node.infra_id
            self.environments[infra_id] = list(
                i for i in self.environments[infra_id]
                if i.id != node_id)
            del self.node_lookup[node_id]
            log.debug("[SC] Done - '%r'", self)

    def create_infrastructure(self, infra_id):
        log.debug("[SC] Creating infrastructure '%s'", infra_id)
        with self.lock:
            self.environments.setdefault(infra_id, dict())
            log.debug("[SC] Done - '%r'", self)

    def drop_infrastructure(self, infra_id):
        if not infra_id in self.environments:
            log.debug('[SC] drop_infrastructure: Infrastructure does not exist; skipping.')
            return
        log.debug("[SC] Dropping infrastructure '%s'", infra_id)
        with self.lock:
            del self.environments[infra_id]
            log.debug("[SC] Done - '%r'", self)

    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[SC] Querying node state for '%s'", node_id)
        with self.lock:
            node = self.node_lookup.get(node_id, None)
            state = 'ready' if node else 'unknown'
            log.debug("[SC] Done - '%s'", state)
        return state

    def get_node_attribute(self, node_id, attribute):
        attrspec = attribute \
            if hasattr(attribute, '__iter__') \
            else attribute.split('.')
        return '{{{{{0}{1}}}}}'.format(node_id,
                               ''.join('[{0}]'.format(i) for i in attrspec))

    def infrastructure_exists(self, infra_id):
        return infra_id in self.environments

    def __repr__(self):
        nodelist_repr = lambda nodelist: ', '.join(repr(n) for n in nodelist)
        envlist_repr = list(
            '%s:[%s]'%(k, nodelist_repr(v))
            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)
