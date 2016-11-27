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

class DummyCommand(Command):
    def __init__(self, retval=None):
        Command.__init__(self)
        self.retval = retval

    def perform(self, cm):
        return self.retval

class ResolveAttributes(Command):
    def __init__(self):
	Command.__init__(self)
	##TODO: fill constructor of new command class

    def perform(self, cm):
	##TODO: Implement perform	


@factory.register(ConfigManager, 'puppet')
class PuppetConfigManager(ConfigManager):

    @util.wet_method()
    def __init__(self, endpoint, auth_data, **cfg):
        log.debug('[CM] Puppet config manager initialisation... done.')
        self.endpoint = endpoint
        self.auth_data = auth_data
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

    def cri_resolve_attributes(self):
	##TODO: fill parameters in both cri_ method and the constructor of command
	return ResolveAttributes()	

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
