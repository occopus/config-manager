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

""" Puppet Config Manager for OCCO

.. moduleauthor:: Gergo Zelenyak <ZZ-05@hotmail.com>

"""

from __future__ import absolute_import

__all__  = [ 'PuppetConfigManager' ]

from occo.configmanager import ConfigManager, Command, CMSchemaChecker
import occo.util as util
import occo.util.factory as factory
import logging

from occo.exceptions import SchemaError
import occo.constants.status as status

PROTOCOL_ID='puppet'

log = logging.getLogger('occo.configmanager')

class GetNodeState(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    
    @util.wet_method('ready')
    def perform(self, cm):
        node_id = self.instance_data['node_id']
        log.debug("[CM] Get node state for %r", node_id)
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))
        log.debug("[CM] Instance_data: %s", str(self.instance_data))
        return status.READY
        #return status.PENDING
        #return status.SHUTDOWN
        #return status.UNKNOWN
        #return status.TMP_FAIL
        #return status.FAIL
        
class GetNodeAttribute(Command):
    def __init__(self, node_id, attribute):
        Command.__init__(self)
        self.node_id = node_id

    @util.wet_method('dummy-value')
    def perform(self, cm):
        log.debug("[CM] Get node attribute: %r", self.resolved_node_definition['name'])
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))
        return 'dummy-value'

class RegisterNode(Command):
    def __init__(self, resolved_node_definition):
        Command.__init__(self)
        self.resolved_node_definition = resolved_node_definition

    @util.wet_method()
    def perform(self, cm):
        log.debug("[CM] Registering node: %r", self.resolved_node_definition['name'])
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))
        cfgmgmtsec = self.resolved_node_definition.get('config_management',None)
        log.debug("[CM] Config management section keys and values: %s",str(cfgmgmtsec))

class DropNode(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
 
    @util.wet_method()
    def perform(self, cm):
        node_id = self.instance_data['node_id']
        log.debug("[CM] Dropping node: %r", node_id)
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))
        log.debug("[CM] Instance_data: %s", str(self.instance_data))

class InfrastructureExists(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method(True)
    def perform(self, cm):
        log.debug("[CM] Infrastructure exists: %r", self.infra_id)
        log.debug("[CM] Infraid: %s",str(self.infra_id))
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))
        return True

class CreateInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method()
    def perform(self, cm):
        log.debug("[CM] Creating environment: %r", self.infra_id)
        log.debug("[CM] Infraid: %s",str(self.infra_id))
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))

class DropInfrastructure(Command):
    def __init__(self, infra_id):
        Command.__init__(self)
        self.infra_id = infra_id
    
    @util.wet_method()
    def perform(self, cm):
        log.debug('[CM] Dropping infrastructure: %r', self.infra_id)
        log.debug("[CM] Infraid: %s",str(self.infra_id))
        log.debug("[CM] Endpoint: %s",str(cm.endpoint))
        log.debug("[CM] Config manager auth_data: %s",str(cm.auth_data))


@factory.register(ConfigManager, 'puppet')
class PuppetConfigManager(ConfigManager):

    @util.wet_method()
    def __init__(self, endpoint, auth_data, **cfg):
        log.debug('[CM] Puppet config manager initialisation... done.')
        self.endpoint = endpoint
        self.auth_data = auth_data
        self.cfg = cfg

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
class PuppetSchemaChecker(CMSchemaChecker):
    def __init__(self):
        self.req_keys = ["type", "endpoint"]
        self.opt_keys = ["manifests", "modules", "variables"]
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
