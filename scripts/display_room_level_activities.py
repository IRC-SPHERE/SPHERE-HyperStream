# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'hyperstream_core'))
sys.path.append(path)
path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'SPHERE-HyperStream'))
sys.path.append(path)
path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'sphere_connector_package'))
sys.path.append(path)
path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'SPHERE-HyperStream/scripts'))
sys.path.append(path)

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from workflows.deploy_room_level_activities import create_workflow_room_level_activities
from hyperstream.utils import UTC

if __name__ == '__main__':
    hs = HyperStream(loglevel=20, file_logger=None)
    M = hs.channel_manager.memory

    workflow_id0 = "sitting_standing_per_room"
display_room_display
    time_interval = TimeInterval(datetime(2017, 7, 21, 00, 00, 0).replace(tzinfo=UTC),
                          datetime(2017, 8, 30, 00, 00, 0).replace(tzinfo=UTC))


    hs.workflow_manager.delete_workflow(workflow_id0)
    try:
        w0 = hs.workflow_manager.workflows[workflow_id0]
    except:
        w0 = create_workflow_sit_stand_per_room(hs, workflow_id0, 3007)

    w0.execute(time_interval)

    for stream_id, stream in M.find_streams(name='average_activity').iteritems():
        print(stream_id)
        for item in stream.window(time_interval).items():
            print (item)