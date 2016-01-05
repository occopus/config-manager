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

""" Service Composer module for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__  = [ 'ServiceComposer' ]

import occo.util.factory as factory
import occo.util as util
import occo.infobroker as ib
import logging

log = logging.getLogger('occo.servicecomposer')

@ib.provider
class ServiceComposer(factory.MultiBackend, ib.InfoProvider):
    """Abstract interface of a service composer.

    .. todo:: Service Composer documentation.
    """

    @ib.provides('node.service.state')
    def service_status(self, instance_data):
        return self.get_node_state(instance_data)

    def get_node_state(self, instance_data):
        raise NotImplementedError()

@factory.register(ServiceComposer, 'dummy')
class DummyServiceComposer(ServiceComposer):
    def __init__(self, dry_run=False, name='SC-dummy', **backend_config):
        self.dry_run = dry_run
        self.name = name

    @util.wet_method()
    def register_node(self, resolved_node_definition):
        return

    @util.wet_method()
    def drop_node(self, instance_data):
        return

    @util.wet_method()
    def create_infrastructure(self, infra_id):
        return

    @util.wet_method(True)
    def infrastructure_exists(self, infra_id):
        return True

    @util.wet_method()
    def drop_infrastructure(self, infra_id):
        return

    @util.wet_method('ready')
    def get_node_state(self, instance_data):
        node_id = instance_data['node_id']
        log.debug("[%s] Querying node state for '%r'", self.name, node_id)
        state = 'ready'
        log.debug("[%s] Done - %r", self.name, state)
        return state

    @util.wet_method('undefined')
    def get_node_attribute(self, node_id, attribute):
        return 'undefined'

