#
# Copyright (C) MTA SZTAKI 2014
#

""" Dummy Service Composer module for OCCO"""

from __future__ import absolute_import

__all__ = [ 'DummyServiceComposer' ]

from occo.servicecomposer import ServiceComposer, Command
import occo.util as util
import occo.util.factory as factory
import logging

import occo.constants.status as status

PROTOCOL_ID='dummy'

log = logging.getLogger('occo.servicecomposer.dummy')

class GetNodeState(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data

    def perform(self, cloud_handler).
        return 'ready'

@factory.register(ServiceComposer, 'dummy')
class DummyServiceComposer(ServiceComposer):
    def __init__(self, name='dummy'):
        self.name = name
    
    def cri_get_node_state(self, instance_data):
        return GetNodeState(instance_data)

    def cri_register_node(self, resolved_node_definition):
        pass

    def cri_drop_node(self, instance_data):
        pass

    def cri_create_infrastructure(self, infra_id):
        pass

    def cri_drop_infrastructure(self, infra_id):
        pass

    def cri_get_node_attribute(self, node_id, attribute):
        pass

    def cri_infra_exists(self, infra_id):
        pass
    
    def perform(self, instruction):
        instruction.perform(self)
