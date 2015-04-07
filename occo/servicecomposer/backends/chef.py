#
# Copyright (C) MTA SZTAKI 2014
#

""" Chef Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__  = [ 'ChefServiceComposer' ]

from occo.servicecomposer import ServiceComposer
import occo.util as util
import occo.util.factory as factory
import logging
import chef
from chef.exceptions import ChefServerNotFoundError

log = logging.getLogger('occo.servicecomposer')

@factory.register(ServiceComposer, 'chef')
class ChefServiceComposer(ServiceComposer):
    def __init__(self, dry_run=False, **backend_config):
        self.dry_run = dry_run
        self.chefapi = chef.ChefAPI(**backend_config)

    def role_name(self, node):
        return '{environment_id}_{name}'.format(**node)
    def node_name(self, node):
        return '{node_id}'.format(**node)
    def bootstrap_recipe_name(self):
        return 'recipe[connect]'

    def list_environments(self):
        return list(chef.Environment.list(api=self.chefapi))

    def list_roles(self):
        return list(chef.Role.list(api=self.chefapi))

    def ensure_role(self, node):
        roles = self.list_roles()
        role = self.role_name(node)
        if role in roles:
            log.debug('Role %r already exists', role)
        else:
            log.info('Registering role %r', role)
            chef.Role(role, api=self.chefapi).save()

    def cond_prepend(self, lst, item):
        if not item in lst:
            lst.insert(0, item)

    def assemble_run_list(self, node):
        """
        .. todo:: This must not be done here. Instead, this belongs to node
            resolution.
        """
        run_list = node['run_list']
        self.cond_prepend(run_list, self.bootstrap_recipe_name())
        self.cond_prepend(run_list, 'role[{0}]'.format(self.role_name(node)))
        return run_list

    def assemble_attributes(self, node, dest_node):
        for key, attributes in node['attributes']:
            a = chef.NodeAttributes()
            for k, v in attributes:
                a.set_dotted(k, v)
            setattr(dest_node, key, a)

    def register_node(self, node):
        log.info("[SC] Registering node: %r", node['name'])

        self.ensure_role(node)

        n = chef.Node(self.node_name(node), api=self.chefapi)
        n.chef_environment = node['environment_id']
        n.run_list = self.assemble_run_list(node)
        self.assemble_attributes(node, n)
        n.save()

        log.debug("[SC] Done")

    def drop_node(self, instance_data):
        node_id = self.node_name(instance_data)
        log.debug("[SC] Dropping node '%s'", node_id)
        chef.Node(node_id).delete()
        log.debug("[SC] Done")

    def environment_exists(self, environment_id):
        return environment_id in self.list_environments()

    def create_environment(self, environment_id):
        log.debug("[SC] Creating environment '%s'", environment_id)
        chef.Environment(environment_id, api=self.chefapi).save()
        log.debug("[SC] Done")

    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)
        try:
            chef.Environment(environment_id, api=self.chefapi).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping environment:')
            log.info('[SC] drop_environment failed - ignoring.')

    def chef_exists(self, chef_object):
        try:
            self.chefapi.api_request('GET', chef_object.url, data=chef_object)
            return True
        except ChefServerNotFoundError:
            return False

    def chef_node_exists(self, node_name):
        node = chef.Node(node_name, api=self.chefapi)
        return self.chef_exists(node)

    @util.wet_method('ready')
    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[SC] Querying node state for '%s'", node_id)
        node = chef.Node(node_id, api=self.chefapi)
        if self.chef_exists(node):
            if 'ohai_time' in node.attributes:
                return 'ready'
            else:
                return 'pending'
        else:
            return 'unknown'

    def get_node_attribute(self, node_id, attribute):
        node = chef.Node(node_id, api=self.chefapi)
        dotted_attr = \
            attribute if type(attribute) is str \
            else '.'.join(attribute) if hasattr(attribute, '__iter__') \
            else util.f_raise(TypeError(
                'Unknown attribute specification: {0}'.format(attribute)))
        try:
            return node.attributes.get_dotted(dotted_attr)
        except KeyError:
            raise KeyError('Unresolved node attribute: %s', dotted_attr)
