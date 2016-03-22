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

.. moduleauthor:: Adam Novak <novak.adam@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__  = [ 'ChefConfigManager' ]

from occo.configmanager import ConfigManager, Command, CMSchemaChecker
import occo.util as util
import occo.util.factory as factory
import logging
import chef
from chef.exceptions import ChefServerNotFoundError
from occo.exceptions import SchemaError
import occo.constants.status as status

PROTOCOL_ID='chef'

log = logging.getLogger('occo.configmanager')

class GetNodeState(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    
    def chef_exists(self, cm, chef_object):
        try:
            cm.chefapi.api_request('GET', chef_object.url, data=chef_object)
            return True
        except ChefServerNotFoundError:
            return False
    
    @util.wet_method('ready')
    def perform(self, cm):
        node_id = self.instance_data['node_id']
        log.debug("[CM] Querying node state for %r", node_id)
        node = chef.Node(node_id, api=cm.chefapi)
        if self.chef_exists(cm, node):
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
    def perform(self, cm):
        node = chef.Node(self.node_id, api=cm.chefapi)
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

    def ensure_role(self, cm):
        roles = cm.list_roles()
        role = cm.role_name(self.resolved_node_definition)
        if role in roles:
            log.debug('Role %r already exists', role)
        else:
            log.info('Registering role %r', role)
            chef.Role(role, api=cm.chefapi).save()

    def cond_prepend(self, lst, item):
        if not item in lst:
            lst.insert(0, item)

    def assemble_run_list(self, cm):
        """
        .. todo:: This must not be done here. Instead, this belongs to node
            resolution.
        """
        run_list = self.resolved_node_definition['config_management']['run_list']
        self.cond_prepend(run_list, cm.bootstrap_recipe_name())
        self.cond_prepend(
            run_list, 'role[{0}]'.format(cm.role_name(self.resolved_node_definition)))
        return run_list

    def assemble_attributes(self, dest_attrs):
        for k, v in self.resolved_node_definition['attributes'].iteritems():
            dest_attrs.set_dotted(k, v)

    @util.wet_method()
    def perform(self, cm):
        log.info("[CM] Registering node: %r", self.resolved_node_definition['name'])

        self.ensure_role(cm)

        n = chef.Node(cm.node_name(self.resolved_node_definition),
                      api=cm.chefapi)
        n.chef_environment = self.resolved_node_definition['infra_id']
        n.run_list = self.assemble_run_list(cm)
        self.assemble_attributes(n.normal)
        n.save()

        log.debug("[CM] Done")

class DropNode(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    
    @util.wet_method()
    def perform(self, cm):
        """
        Delete a node and all associated data from the chef server.

        .. todo:: Delete the generated client too.
        """
        node_id = cm.node_name(self.instance_data)
        log.debug("[CM] Dropping node %r", node_id)
        try:
            chef.Node(node_id, api=cm.chefapi).delete()
            log.debug("[CM] Done")
        except Exception as ex:
            log.exception('Error dropping node:')
            log.info('[CM] Dropping node failed - ignoring.')

class InfrastructureExists(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method(True)
    def perform(self, cm):
        return self.infra_id in cm.list_environments()

class CreateInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method()
    def perform(self, cm):
        log.debug("[CM] Creating environment %r", self.infra_id)
        chef.Environment(self.infra_id, api=cm.chefapi).save()
        log.debug("[CM] Done")

class DropInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method()
    def perform(self, cm):
        """
        Delete the environment and associated data.

        """
        filter = '{0}_'.format(self.infra_id)
        for role in cm.list_roles():
            if role.startswith(filter):
                log.debug("[CM] Removing role: %r", role)
                try:
                    chef.Role(role, api=cm.chefapi).delete()
                    log.debug("[CM] Done")
                except Exception as ex:
                    log.exception('Error removing role:')
                    log.info('[CM] Removing role failed - ignoring.')

        log.debug("[CM] Dropping environment %r", self.infra_id)
        try:
            chef.Environment(self.infra_id, api=cm.chefapi).delete()
            log.debug("[CM] Done")
        except Exception as ex:
            log.exception('Error dropping environment:')
            log.info('[CM] drop_infrastructure failed - ignoring.')


@factory.register(ConfigManager, 'chef')
class ChefConfigManager(ConfigManager):
    """
    Chef implementation of :class:`occo.configmanager.ConfigManager`.

    .. todo:: Store instance name too so it can be used in logging.
    """
    def __init__(self, endpoint, auth_data, **cfg):
        config = dict()
        config['client'] = auth_data['client_name']
        config['key'] = auth_data['client_key']
        config['url'] = endpoint
        self.chefapi = chef.ChefAPI(**config)

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

@factory.register(CMSchemaChecker, PROTOCOL_ID)
class ChefSchemaChecker(CMSchemaChecker):
    def __init__(self):
#        super(__init__(), self)
        self.req_keys = ["type", "endpoint", "run_list"]
        self.opt_keys = []
    def perform_check(self, data):
        missing_keys = CMSchemaChecker.get_missing_keys(self, data, self.req_keys)
        if missing_keys:
            msg = "Missing key(s): " + ', '.join(str(key) for key in missing_keys)
            raise SchemaError(msg)
        valid_keys = self.req_keys + self.opt_keys
        invalid_keys = CMSchemaChecker.get_invalid_keys(self, data, valid_keys)
        if invalid_keys:
            msg = "Unknown key(s): " + ', '.join(str(key) for key in invalid_keys)
            raise SchemaError(msg)
        return True
