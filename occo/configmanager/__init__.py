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
    def __init__(self):
        self.infobroker = ib.main_info_broker
        self.config_managers = None
        return

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

    def instantiate_cm(self, data):
        cfg = data.get('config_management')
        if not cfg:
            cfg = data['resolved_node_definition']['config_management']
        return ConfigManager.instantiate(protocol=cfg['type'],**cfg)

    def register_node(self, resolved_node_definition):
        cm = self.instantiate_cm(resolved_node_definition)
        return cm.cri_register_node(resolved_node_definition).perform(cm)

    def drop_node(self, instance_data):
        cm = self.instantiate_cm(instance_data)
        return cm.cri_drop_node(instance_data).perform(cm)

    def get_node_state(self, instance_data):
        cm = self.instantiate_cm(instance_data)
        return cm.cri_get_node_state(instance_data).perform(cm)

    def create_infrastructure(self, infra_id):
        log.debug("[SC]Building necessary environments for infrastructure %r", infra_id)
        self.config_managers = self.infobroker.get('config_managers',infra_id) if self.config_managers is None else self.config_managers
	for cfg in self.config_managers:
            cm = ConfigManager.instantiate(protocol=cfg['type'],**cfg)
            cm.cri_create_infrastructure(infra_id).perform(cm)

    def drop_infrastructure(self, infra_id):
        log.debug("[SC]Destroying environments for infrastructure %r", infra_id)
        self.config_managers = self.infobroker.get('config_managers', infra_id) if self.config_managers is None else self.config_managers
        for cfg in self.config_managers:
            cm = ConfigManager.instantiate(protocol=cfg['type'],**cfg)
            cm.cri_drop_infrastructure(infra_id).perform(cm)

    def infrastructure_exists(self, infra_id):
        log.debug("[SC]Checking necessary environments for infrastructure %r", infra_id)
        self.config_managers = self.infobroker.get('config_managers', infra_id) if self.config_managers is None else self.config_managers
        for cfg in self.config_managers:
            cm = ConfigManager.instantiate(protocol=cfg['type'],**cfg)
            retval = cm.cri_infrastructure_exists(infra_id).perform(cm)
            if retval is False:
                log.debug("[CM] Environment for %r (%r) is not ready", cfg['type'], cfg['endpoint'])
                break
            else:
                log.debug("[CM] Environment for %r (%r) is ready", cfg['type'], cfg['endpoint'])
        return retval

    def get_node_attribute(self, node_id, attribute):
        node = self.infobroker.get('node.find_one', node_id = node_id)
        cfg = node['resolved_node_definition']
        cm = self.instantiate_cm(cfg)
        return cm.cri_get_node_attribute(node_id, attribute).perform(cm)
