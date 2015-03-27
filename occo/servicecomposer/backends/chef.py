#
# Copyright (C) MTA SZTAKI 2014
#

""" Chef Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__  = [ 'ChefServiceComposer' ]

from occo.servicecomposer import ServiceComposer
import occo.util.factory as factory
import logging
import chef

log = logging.getLogger('occo.servicecomposer')

@factory.register(ServiceComposer, 'chef')
class ChefServiceComposer(ServiceComposer):
    def __init__(self, **backend_config):
        self.chefapi = chef.ChefAPI(**backend_config)

    def role_name(self, node):
        return '{environment_id}_{name}'.format(**node)

    def list_roles(self):
        return list(chef.Role.list(api=self.chefapi))

    def register_role(self, node):
        roles = self.list_roles()
        role = self.role_name(node)
        if role in roles:
            log.debug('Role %r already exists', role)
        else:
            log.info('Registering role %r', role)
            chef.Role(role, api=self.chefapi).save()

    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node['name'])

        self.register_role(node)
        role = chef.Role(node['name'])
        n = chef.Node(node['id'], api=self.chefapi)
        n.run_list = []
        with self.lock:
            envid = node['environment_id']

            # Implicitly create an environment for individual nodes.
            # (May not be useful for real SCs!)
            if not envid in self.environments:
                self.create_environment(envid)

            self.environments[envid].setdefault(node['name'], list()).append(node)
            self.node_lookup[node['id']] = node
            log.debug("[SC] Done - '%r'", self)
    def drop_node(self, instance_data):
        node_id = instance_data['instance_id']
        if not node_id in self.node_lookup:
            log.debug('[SC] drop_node: Node does not exist; skipping.')
            return

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
        chef.Environment(envinroment_id, api=self.chefapi).save()
        log.debug("[SC] Done")

    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)
        try:
            chef.Environment(environment_id, api=self.chefapi).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping environment:')
            log.info('[SC] drop_environment failed - ignoring.')

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
    def __repr__(self):
        log.info('%r', self.environments)
        nodelist_repr = lambda nodelist: ', '.join(repr(n) for n in nodelist)
        envlist_repr = list(
            '%s:[%s]'%(k, nodelist_repr(v))
            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)
