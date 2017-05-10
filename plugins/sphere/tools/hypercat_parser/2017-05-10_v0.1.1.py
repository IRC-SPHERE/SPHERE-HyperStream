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

from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.stream import StreamInstance


ENV = ["temperature", "humidity", "pressure", "water", "light", "motion"]


class HypercatParser(Tool):
    """
    Tool for parsing the HyperCat into the format understood by the SPHERE Assets channel
    """

    def __init__(self, house):
        super(HypercatParser, self).__init__(house=house)

    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        items = sources[0].window(interval, force_calculation=True).items()

        # TODO: Update the last element of the existing assets stream
        assets = deepcopy(sources[1].window(interval).last().value)

        assets['house'][self.house] = {
            'access_points': {},
            'cameras': {},
            'env_sensors': {},
            'wearables': {},
            'notes': "Auto-generated from HyperCat"
        }

        from collections import defaultdict
        a_locs = defaultdict(int)
        c_locs = defaultdict(int)

        for item in items:
            uid = item[1]['href'].lower()
            metadata = item[1][u'metadata']
            device_type = metadata[u'device_type']
            description = metadata[u'urn:X-tsbiot:rels:hasDescription:en']
            location = metadata[u'house_location'] if 'house_location' in metadata else None

            # Note possible typo in description
            if device_type == u'SPES2' or description in {u'Enviromental Sensor', u'Environmental Sensor'}:
                assets['house'][self.house]['env_sensors'][uid] = ENV

            elif device_type == u'Current Cost Appliance' or description == u'Power Usage Monitor':
                assets['house'][self.house]['env_sensors'][uid] = ["electricity"]

            elif device_type == u'SPG2_F' or description == u'Sphere Gateway':
                a_locs[location] += 1
                assets['house'][self.house]['access_points'][uid] = \
                    location + ":" + chr(a_locs[location] - 1 + ord("A"))

            elif device_type == u'SPG2_BR' or description == u'Root Sphere Gateway':
                assets['house'][self.house]['access_points'][uid] = "root:A"

            elif device_type == u'Intel NUC VIDEO' or description == u'Motion Detector':
                c_locs[location] += 1
                assets['house'][self.house]['cameras'][uid] = \
                    location + ":" + chr(c_locs[location] - 1 + ord("A"))

                # Also use as access point
                uid_with_colons = uid[0:2]+':'+uid[2:4]+':'+uid[4:6]+':'+uid[6:8]+':'+uid[8:10]+':'+uid[10:12]
                a_locs[location] += 1
                assets['house'][self.house]['access_points'][uid_with_colons] = \
                    location + ":" + chr(a_locs[location] - 1 + ord("A"))

            elif device_type == u'Intel NUC HOME' or description == u'Home Gateway':
                pass

            elif device_type == u'SPW2' or description == u'Wearable Sensor':
                wid = chr(ord('A') + ord(uid[-1]) - ord('0'))
                assets['house'][self.house]['wearables'][uid] = wid

            else:
                logging.debug('Unknown device type {}: {}'.format(device_type, description))

        yield StreamInstance(items[0].timestamp, assets)
