### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

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

PROTOCOL_ID='chef'

log = logging.getLogger('occo.servicecomposer')

class GetNodeState(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    
    def chef_exists(self, sc, chef_object):
        try:
            sc.chefapi.api_request('GET', chef_object.url, data=chef_object)
            return True
        except ChefServerNotFoundError:
            return False
    
    @util.wet_method('ready')
    def perform(self, sc):
        node_id = self.instance_data['node_id']
        log.debug("[SC] Querying node state for %r", node_id)
        node = chef.Node(node_id, api=sc.chefapi)
        if self.chef_exists(sc, node):
            if 'ohai_time' in node.attributes:
                return status.READY
            else:
                return status.PENDING
        else:
            return status.UNKNOWN
        
class GetNodeAttribute(Command):
    def __init__(self, node_id, attribute):
        Command.__init__(self)
        self.node_id = node_id
        self.attribute = attribute

    @util.wet_method('dummy-value')
    def perform(self, sc):
        node = chef.Node(self.node_id, api=sc.chefapi)
        dotted_attr = \
            attribute if isinstance(self.attribute, basestring) \
            else '.'.join(self.attribute) if hasattr(self.attribute, '__iter__') \
            else util.f_raise(TypeError(
                'Unknown attribute specification: {0}'.format(self.attribute)))
        try:
            return node.attributes.get_dotted(dotted_attr)
        except KeyError:
            raise KeyError('Unresolved node attribute: %s', dotted_attr)

class RegisterNode(Command):
    def __init__(self, resolved_node_definition):
        Command.__init__(self)
        self.resolved_node_definition = resolved_node_definition

    def ensure_role(self, sc):
        roles = sc.list_roles()
        role = sc.role_name(resolved_node_definition)
        if role in roles:
            log.debug('Role %r already exists', role)
        else:
            log.info('Registering role %r', role)
            chef.Role(role, api=sc.chefapi).save()

    def cond_prepend(self, lst, item):
        if not item in lst:
            lst.insert(0, item)

    def assemble_run_list(self, sc):
        """
        .. todo:: This must not be done here. Instead, this belongs to node
            resolution.
        """
        run_list = resolved_node_definition['run_list']
        self.cond_prepend(run_list, sc.bootstrap_recipe_name())
        self.cond_prepend(
            run_list, 'role[{0}]'.format(sc.role_name(self.resolved_node_definition)))
        return run_list

    def assemble_attributes(self, dest_attrs):
        for k, v in self.resolved_node_definition['attributes'].iteritems():
            dest_attrs.set_dotted(k, v)

    def perform(self, sc):
        log.info("[SC] Registering node: %r", self.resolved_node_definition['name'])

        self.ensure_role(sc)

        n = chef.Node(sc.node_name(self.resolved_node_definition),
                      api=sc.chefapi)
        n.chef_environment = self.resolved_node_definition['infra_id']
        n.run_list = self.assemble_run_list(sc)
        self.assemble_attributes(n.normal)
        n.save()

        log.debug("[SC] Done")

class DropNode(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    
    def perform(self, sc):
        """
        Delete a node and all associated data from the chef server.

        .. todo:: Delete the generated client too.
        """
        node_id = sc.node_name(self.instance_data)
        log.debug("[SC] Dropping node %r", node_id)
        try:
            chef.Node(node_id).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping node:')
            log.info('[SC] Dropping node failed - ignoring.')

class InfrastructureExists(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    def perform(self, sc):
        return infra_id in sc.list_environments()

class CreateInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    def perform(self, sc):
        log.debug("[SC] Creating environment %r", self.infra_id)
        chef.Environment(self.infra_id, api=sc.chefapi).save()
        log.debug("[SC] Done")

class DropInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    def perform(self, sc):
        """
        Delete the environment and associated data.

        """
        filter = '{0}_'.format(infra_id)
        for role in sc.list_roles():
            if role.startswith(filter):
                log.debug("[SC] Removing role: %r", role)
                try:
                    chef.Role(role, api=sc.chefapi).delete()
                    log.debug("[SC] Done")
                except Exception as ex:
                    log.exception('Error removing role:')
                    log.info('[SC] Removing role failed - ignoring.')

        log.debug("[SC] Dropping environment %r", self.infra_id)
        try:
            chef.Environment(self.infra_id, api=sc.chefapi).delete()
            log.debug("[SC] Done")
        except Exception as ex:
            log.exception('Error dropping environment:')
            log.info('[SC] drop_infrastructure failed - ignoring.')


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

    def cri_drop_infrastructure(self, infra_id):
        return DropInfrastructure(infra_id)

    def cri_create_infrastructure(self, infra_id):
        return CreateInfrastructure(infra_id)
    
    def cri_infrastructure_exists(self, infra_id):
        return InfrastructureExists(infra_id)

    def cri_register_node(self, resolved_node_definition):
        return RegisterNode(resolved_node_definition)

    def cri_drop_node(self, instance_data):
        return DropNode(instance_data)

    def cri_get_node_state(self, instance_data):
        return GetNodeState(instance_data)

    def cri_get_node_attribute(self, node_id, attribute):
        return GetNodeAttribute(node_id, attribute)

    def perform(self, instruction):
        instruction.perform(self)
