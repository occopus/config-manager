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

""" Dummy Configuration Manager module for OCCO

.. moduleauthor:: Adam Novak <novak.adam@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__ = [ 'DummyConfigManager' ]

from occo.configmanager import ConfigManager, Command
import occo.util as util
import occo.util.factory as factory
import logging

import occo.constants.status as status

PROTOCOL_ID='dummy'

log = logging.getLogger('occo.configmanager.dummy')


class DummyCommand(Command):
    def __init__(self, retval=None):
        Command.__init__(self)
	self.retval = retval

    def perform(self, cm):
	return self.retval	

@factory.register(ConfigManager, 'dummy')
class DummyConfigManager(ConfigManager):
    def __init__(self, name='dummy', **kwargs):
        self.name = name
    
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
  
    def perform(self, instruction):
        instruction.perform(self)
