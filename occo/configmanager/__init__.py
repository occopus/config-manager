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

""" Configuration Manager module for OCCO

.. moduleauthor:: Adam Novak <novak.adam@sztaki.mta.hu>

"""

__all__  = [ 'ConfigManager', 'ConfigManagerProvider' ]

import occo.util.factory as factory
import occo.util as util
import occo.infobroker as ib
import logging

log = logging.getLogger('occo.configmanager')

class Command(object):
    def __init__(self):
        pass
    def perform(self, config_manager):
        raise NotImplementedError()

@ib.provider
class ConfigManagerProvider(ib.InfoProvider):
    """Abstract interface of a config manager provider.

    .. todo:: Service Composer documentation.
    """
    def __init__(self, config_manager, **config):
        self.__dict__.update(config)
        self.config_manager = config_manager

    @ib.provides('node.service.state')
    def service_status(self, instance_data):
        return self.config_manager.get_node_state(instance_data)

class ConfigManager(factory.MultiBackend):
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
        scid = data.get('config_manager_id')
        if not scid:
            scid = data['resolved_node_definition']['config_manager_id']
        cfg = self.sc_cfgs[scid]
        return ConfigManager.instantiate(**cfg)

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
        log.debug("[SC]Building necessary environments for infrastructure %r", infra_id)
        for key in self.sc_cfgs:
            cfg = self.sc_cfgs[key]
            sc = ConfigManager.instantiate(**cfg)
            sc.cri_create_infrastructure(infra_id).perform(sc)

    def drop_infrastructure(self, infra_id):
        log.debug("[SC]Destroying environments for infrastructure %r", infra_id)
        for key in self.sc_cfgs:
            cfg = self.sc_cfgs[key]
            sc = ConfigManager.instantiate(**cfg)
            sc.cri_drop_infrastructure(infra_id).perform(sc)

    def infrastructure_exists(self, infra_id):
        log.debug("[SC]Checking necessary environments for infrastructure %r", infra_id)
        retval = True
        for key in self.sc_cfgs:
            cfg = self.sc_cfgs[key]
            sc = ConfigManager.instantiate(**cfg)
            retval = sc.cri_infrastructure_exists(infra_id).perform(sc)
            if retval is False:
                log.debug("[SC] Environment for %r is not ready", key)
                break
            else:
                log.debug("[SC] Environment for %r is ready", key)
        return retval

    def get_node_attribute(self, node_id, attribute):
        node = self.infobroker.get('node.find_one', node_id = node_id)
        sc_id = node['resolved_node_definition']['config_manager_id']
        cfg = self.sc_cfgs[sc_id]
        sc = ConfigManager.instantiate(**cfg)
        return sc.cri_get_node_attribute(node_id, attribute).perform(sc)
