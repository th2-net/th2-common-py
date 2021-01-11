#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import json
import os


class PrometheusConfiguration:
    host = '0.0.0.0'
    port = 9752
    enabled = True

    # CONFIG_DEFAULT_PATH = '/var/th2/config/'
    PROMETHEUS_CONFIG_FILENAME = 'prometheus.json'
    GENERATED_CONFIG_PATH = os.path.join('generated_configs', PROMETHEUS_CONFIG_FILENAME)

    def __init__(self, config_path=GENERATED_CONFIG_PATH):

        with open(config_path, "r") as file:
            data = json.load(file)
            config = json.loads(data)
            self.host = config['host']
            self.port = config['port']
            self.enabled = config['enabled']





