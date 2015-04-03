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
from chef.exceptions import ChefServerNotFoundError

log = logging.getLogger('occo.servicecomposer')

@factory.register(ServiceComposer, 'chef')
class ChefServiceComposer(ServiceComposer):
    def __init__(self, **backend_config):
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
        run_list = node['run_list']
        self.cond_prepend(run_list, self.bootstrap_recipe_name())
        self.cond_prepend(run_list, self.role_name(node))
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

        log.debug("[SC] Done", self)

    def drop_node(self, instance_data):
        log.debug("[SC] Dropping node '%s'", node_id)
        chef.Node(self.node_name(instance_data)).delete()
        log.debug("[SC] Done - '%r'", self)

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

    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[SC] Querying node state for '%s'", node_id)
        node = chef.Node(node_id, api=self.chefapi)
        if self.chef_exists(node):
            try:
                if 'ohai_time' in node.attributes:
                    return 'ready'
                else:
                    return 'pending'
            except KeyError:
                return 'unknown'
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
