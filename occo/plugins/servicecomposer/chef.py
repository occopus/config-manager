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

import occo.constants.status as status

log = logging.getLogger('occo.servicecomposer')

@factory.register(ServiceComposer, 'chef')
class ChefServiceComposer(ServiceComposer):
    """
    Chef implementation of :class:`occo.servicecomposer.ServiceComposer`.

    .. todo:: Store instance name too so it can be used in logging.
    """
    def __init__(self, dry_run=False, **backend_config):
        self.dry_run = dry_run
        self.chefapi = chef.ChefAPI(**backend_config)

    def role_name(self, resolved_node_definition):
        return '{infra_id}_{name}'.format(**resolved_node_definition)
    def node_name(self, resolved_node_definition):
        return '{node_id}'.format(**resolved_node_definition)
    def bootstrap_recipe_name(self):
        return 'recipe[connect]'

    def list_environments(self):
        log.debug('Listing environments')
        return list(chef.Environment.list(api=self.chefapi))

    def list_roles(self):
        log.debug('Listing roles')
        return list(chef.Role.list(api=self.chefapi))

    def ensure_role(self, resolved_node_definition):
        roles = self.list_roles()
        role = self.role_name(resolved_node_definition)
        if role in roles:
            log.debug('Role %r already exists', role)
        else:
            log.info('Registering role %r', role)
            chef.Role(role, api=self.chefapi).save()

    def cond_prepend(self, lst, item):
        if not item in lst:
            lst.insert(0, item)

    def assemble_run_list(self, resolved_node_definition):
        """
        .. todo:: This must not be done here. Instead, this belongs to node
            resolution.
        """
        run_list = resolved_node_definition['run_list']
        self.cond_prepend(run_list, self.bootstrap_recipe_name())
        self.cond_prepend(
            run_list, 'role[{0}]'.format(self.role_name(resolved_node_definition)))
        return run_list

    def assemble_attributes(self, resolved_node_definition, dest_attrs):
        for k, v in resolved_node_definition['attributes'].iteritems():
            dest_attrs.set_dotted(k, v)

    def register_node(self, resolved_node_definition):
        log.info("[SC] Registering node: %r", resolved_node_definition['name'])

        self.ensure_role(resolved_node_definition)

        n = chef.Node(self.node_name(resolved_node_definition),
                      api=self.chefapi)
        n.chef_environment = resolved_node_definition['infra_id']
        n.run_list = self.assemble_run_list(resolved_node_definition)
        self.assemble_attributes(resolved_node_definition, n.normal)
        n.save()

        log.debug("[SC] Done")

    def drop_node(self, instance_data):
        """
        Delete a node and all associated data from the chef server.

        .. todo:: Delete the generated client too.
        """
        node_id = self.node_name(instance_data)
        log.debug("[SC] Dropping node %r", node_id)
        try:
            chef.Node(node_id).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping node:')
            log.info('[SC] Dropping node failed - ignoring.')

    def infrastructure_exists(self, infra_id):
        return infra_id in self.list_environments()

    def create_infrastructure(self, infra_id):
        log.debug("[SC] Creating environment %r", infra_id)
        chef.Environment(infra_id, api=self.chefapi).save()
        log.debug("[SC] Done")

    def drop_infrastructure(self, infra_id):
        """
        Delete the environment and associated data.

        """
        filter = '{0}_'.format(infra_id)
        for role in self.list_roles():
            if role.startswith(filter):
                log.debug("[SC] Removing role: %r", role)
                try:
                    chef.Role(role, api=self.chefapi).delete()
                    log.debug("[SC] Done")
                except Exception as ex:
                    log.exception('Error removing role:')
                    log.info('[SC] Removing role failed - ignoring.')

        log.debug("[SC] Dropping environment %r", infra_id)
        try:
            chef.Environment(infra_id, api=self.chefapi).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping environment:')
            log.info('[SC] drop_infrastructure failed - ignoring.')

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
        log.debug("[SC] Querying node state for %r", node_id)
        node = chef.Node(node_id, api=self.chefapi)
        if self.chef_exists(node):
            if 'ohai_time' in node.attributes:
                return status.READY
            else:
                return status.PENDING
        else:
            return status.UNKNOWN

    @util.wet_method('dummy-value')
    def get_node_attribute(self, node_id, attribute):
        node = chef.Node(node_id, api=self.chefapi)
        dotted_attr = \
            attribute if isinstance(attribute, basestring) \
            else '.'.join(attribute) if hasattr(attribute, '__iter__') \
            else util.f_raise(TypeError(
                'Unknown attribute specification: {0}'.format(attribute)))
        try:
            return node.attributes.get_dotted(dotted_attr)
        except KeyError:
            raise KeyError('Unresolved node attribute: %s', dotted_attr)
