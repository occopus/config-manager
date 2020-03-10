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

""" Puppet Solo Config Manager for OCCO

.. moduleauthor:: Gergo Zelenyak <ZZ-05@hotmail.com>

"""



__all__  = [ 'PuppetSoloConfigManager' ]

from occo.configmanager import ConfigManager, Command, CMSchemaChecker
import occo.util as util
import occo.util.factory as factory
import logging

from occo.exceptions import SchemaError
import occo.constants.status as status

PROTOCOL_ID='puppet_solo'

log = logging.getLogger('occo.configmanager')

class DummyCommand(Command):
    def __init__(self, retval=None):
        Command.__init__(self)
        self.retval = retval

    def perform(self, cm):
        return self.retval

class ResolveAttributes(Command):
    def __init__(self, node_def):
        Command.__init__(self)
        self.node_def = node_def

    def perform(self, cm):
        cm_section = self.node_def.get('config_management')
        attributes=dict()
        attributes['puppet']=dict()
        #HERE COMES adding string content to the 3 sections, based on value of cm_section
        attributes['puppet']['modules']=""
        attributes['puppet']['manifests']=""
        attributes['puppet']['attributes']=""
        #Create modules string
        modules_dict = cm_section.get('modules',None)
        if modules_dict:
           attributes['puppet']['modules'] = ' '.join([ str(k) for k in cm_section.get('modules',dict())])
        log.debug("Puppet solo config manager attributes modules string: %r\n",attributes['puppet']['modules'])
        #Create manifests string
        manifests_dict = cm_section.get('manifests',None)
        if manifests_dict:
           attributes['puppet']['manifests'] = ' '.join([ str(k) for k in cm_section.get('manifests',dict())])
        log.debug("Puppet solo config manager attributes manifests string: %r\n",attributes['puppet']['manifests'])
        #Create attributes string
        attributes_dict = cm_section.get('attributes',None)
        if attributes_dict:
           attributes['puppet']['attributes'] = ' '.join([ str(k) for k in cm_section.get('attributes',dict())])
        log.debug("Puppet solo config manager attributes string: %r\n",attributes['puppet']['attributes'])

        return attributes

@factory.register(ConfigManager, PROTOCOL_ID)
class PuppetSoloConfigManager(ConfigManager):

    @util.wet_method()
    def __init__(self, **cfg):
        log.debug('[Puppet Solo] Puppet solo config manager initialisation... done.')
        self.cfg = cfg

    def cri_drop_infrastructure(self, infra_id):
        return DummyCommand()

    def cri_create_infrastructure(self, infra_id):
        return DummyCommand()

    def cri_infrastructure_exists(self, infra_id):
        return DummyCommand(True)

    def cri_register_node(self, resolved_node_definition):
        return DummyCommand()

    def cri_drop_node(self, instance_data):
        return DummyCommand()

    def cri_get_node_state(self, instance_data):
        return DummyCommand("ready")

    def cri_get_node_attribute(self, node_id, attribute):
        return DummyCommand("dummy attribute")

    def cri_resolve_attributes(self, node_def):
        ##TODO: fill parameters in both cri_ method and the constructor of command
        return ResolveAttributes(node_def)

    def perform(self, instruction):
        instruction.perform(self)

@factory.register(CMSchemaChecker, PROTOCOL_ID)
class PuppetSchemaChecker(CMSchemaChecker):
    def __init__(self):
        self.req_keys = ["type", "manifests"]
        self.opt_keys = ["modules", "attributes"]
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
