# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.

from copy import deepcopy
import logging
from collections import defaultdict

from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.stream import StreamInstance


ENV = ["temperature", "humidity", "pressure", "water", "light", "motion"]
AP_ENV = ["water", "temperature", "humidity"]


def rel_val_list_to_dict(x):
    res = dict()
    for d in x:
        res[d['rel']] = d['val']
    return res


class HypercatUidMapper(Tool):
    """
    Tool to supplement the parsing the HyperCat into the format understood by the SPHERE Assets channel
    (see hypercat_parser).

    This will create a separate stream of mappings from uid's to locations and capabilities
    """

    def __init__(self, house):
        super(HypercatUidMapper, self).__init__(house=house)

    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        items = sources[0].window(interval, force_calculation=True).items()

        # TODO: Update the last element of the existing assets stream
        assets = deepcopy(sources[1].window(interval).last().value)

        if self.house == "all":
            assets_timestamp = items[-1].timestamp
        else:
            assets_timestamp = items[0].timestamp
            items = [(assets_timestamp, dict(house=self.house, hypercat=[data for (time, data) in items]))]

        for (timestamp, house_and_hypercat) in items:
            house = house_and_hypercat['house']
            hypercat_items = house_and_hypercat['hypercat']
            assets['house'][house] = defaultdict(dict)
            assets['house'][house]['notes'] = "Auto-generated from HyperCat"

            def add_device(device_type_readable, device=None, root_gateway=False):
                if device_type_readable:
                    d = {'location': location}
                    if appliance:
                        d['appliance'] = appliance
                    if device is not None:
                        d['device'] = device
                        d['root_gateway'] = root_gateway
                    assets['house'][house][device_type_readable][uid] = d

            for item in hypercat_items:
                uid = item['href'].lower()
                if self.house == "all":
                    metadata = rel_val_list_to_dict(item[u'i-object-metadata'])
                else:
                    metadata = item[u'metadata']

                device_type = metadata[u'device_type']
                description = metadata[u'urn:X-tsbiot:rels:hasDescription:en']
                location = metadata.get(u'house_location', None)
                appliance = metadata.get(u'appliance', None)

                # Note possible typo in description
                # noinspection SpellCheckingInspection
                if device_type == u'SPES2' or description in {u'Enviromental Sensor', u'Environmental Sensor'}:
                    add_device('env_sensors')

                elif device_type == u'SPAQ2':
                    add_device('water_sensors_wet')

                elif device_type == u'SPAQ2_P':
                    add_device('water_sensors_dry')

                elif device_type == u'Current Cost Appliance' or description == u'Power Usage Monitor':
                    add_device('electricity_sensors')

                elif device_type == u'SPG2_F' or description == u'Sphere Gateway':
                    add_device('access_points', device="gateway")
                    # Also use as environmental sensor
                    add_device('env_sensors')

                elif device_type == u'SPG2_BR' or description == u'Root Sphere Gateway':
                    add_device('access_points', device="gateway", root_gateway=True)

                elif device_type == u'Intel NUC VIDEO' or description == u'Motion Detector':
                    add_device('cameras')
                    # Also use as access point with modified uid
                    uid = ':'.join([uid[0:2], uid[2:4], uid[4:6], uid[6:8], uid[8:10], uid[10:12]])
                    add_device('access_points', device="videonuc")

                elif device_type == u'Intel NUC HOME' or description == u'Home Gateway':
                    pass

                elif device_type == u'SPW2' or description == u'Wearable Sensor':
                    # No location for wearables
                    pass

                else:
                    logging.debug('Unknown device type {}: {}'.format(device_type, description))

        yield StreamInstance(assets_timestamp, assets)
