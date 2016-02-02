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

__all__  = [ 'ServiceComposer', 'ServiceComposerProvider' ]

import occo.util.factory as factory
import occo.util as util
import occo.infobroker as ib
import logging

log = logging.getLogger('occo.servicecomposer')

class Command(object):
    def __init__(self):
        pass
    def perform(self, service_composer):
        raise NotImplementedError()

@ib.provider
class ServiceComposerProvider(ib.InfoProvider):
    """Abstract interface of a service composer provider.

    .. todo:: Service Composer documentation.
    """
    def __init__(self, sc, **config):
        self.__dict__.update(config)
        self.service_composer = sc

    @ib.provides('node.service.state')
    def service_status(self, instance_data):
        return self.service_composer.get_node_state(instance_data)

class ServiceComposer(factory.Multibackend):
    def __init__(self, sc_cfgs):
        self.sc_cfgs = sc_cfgs
        self.infobroker = ib.main_info_broker
    def cri_register_node(self, resolved_node_definition):
        raise NotImplementedError()

    def cri_drop_node(self, instance_data):
        raise NotImplementedError()

    def cri_get_node_state(self, instance_data):
        raise NotImplementedError()

    def cri_create_infrastructure(self, infra_id):
        raise NotImplementedError()

    def cri_drop_infrastructure(self, infra_id):
        raise NotImplementedError()

    def cri_get_node_attribute(self, node_id, attribute):
        raise NotImplementedError()

    def cri_infra_exists(self, infra_id):
        raise NotImplementedError()

    def instantiate_sc(self, data):
        cfg = self.sc_cfgs[data['service_composer_id']]
        return ServiceComposer.instantiate(**cfg)

    def register_node(self, resolved_node_definition):
        sc = self.instantiate_sc(resolved_node_definition)
        return sc.cri_register_node(resolved_node_definition).perform(sc)

    def drop_node(self, instance_data):
        sc = self.instantiate_sc(instance_data)
        return sc.cri_drop_node(instance_data).perform(sc)

    def get_node_state(self, instance_data):
        sc = self.instantiate_sc(instance_data)
        return sc.cri_get_node_state(instance_data).perform(sc)

    def create_infrastructure(self, infra_id):
        instances = self.infobroker.get('infrastructure.node_instances',
                                    infra_id, True)
        sclist = []
        for node in instances.itervalues():
            for instance in node.itervalues():
                sc_id = instance['resolved_node_definition']['service_composer_id']
                if sc_id not in sclist:
                    sclist.append(sc_id)
        for sc_id in sclist:
            cfg = self.sc_cfgs[sc_id]
            sc = ServiceComposer.instantiate(**cfg)
            sc.cri_create_infrastructure(infra_id).perform(sc)

    def drop_infrastructure(self, infra_id):
        instances = self.infobroker.get('infrastructure.node_instances',
                                    infra_id, True)
        sclist = []
        for node in instances.itervalues():
            for instance in node.itervalues():
                sc_id = instance['resolved_node_definition']['service_composer_id']
                if sc_id not in sclist:
                    sclist.append(sc_id)
        for sc_id in sclist:
            cfg = self.sc_cfgs[sc_id]
            sc = ServiceComposer.instantiate(**cfg)
            sc.cri_drop_infrastructure(infra_id).perform(sc)

    def infrastructure_exists(self, infra_id):
        instances = self.infobroker.get('infrastructure.node_instances',
                                    infra_id, True)
        sclist = []
        retval = True
        for node in instances.itervalues():
            for instance in node.itervalues():
                sc_id = instance['resolved_node_definition']['service_composer_id']
                if sc_id not in sclist:
                    sclist.append(sc_id)
        for sc_id in sclist:
            cfg = self.sc_cfgs[sc_id]
            sc = ServiceComposer.instantiate(**cfg)
            retval = sc.cri_infrastructure_exists(infra_id).perform(sc)
            if retval is False:
                break
        return retval

    def get_node_attribute(self, node_id, attribute):
        node = self.infobroker.get('node.find_one', node_id = node_id)
        sc_id = node['resolved_node_definition']['service_composer_id']
        cfg = self.sc_cfgs[sc_id]
        sc = ServiceComposer.instantiate(**cfg)
        return sc.cri_get_node_attribute(node_id, attribute).perform(sc)

