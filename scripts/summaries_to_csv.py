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

import logging
from dateutil.parser import parse
import pandas as pd

globs = {'sphere_connector': None}


def run(delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from workflows.summaries_to_csv import create_workflow_summaries_to_csv
    from sphere_connector_package.sphere_connector import SphereConnector

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    hyperstream = HyperStream(loglevel=loglevel, file_logger=None)

    workflow_id = "summaries_to_csv"
    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)
    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        # percentile_results = []
        # w = create_workflow_summaries_to_csv(hyperstream,percentile_results=percentile_results,safe=False)
        w = create_workflow_summaries_to_csv(hyperstream,safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    day_str = "2016_12_15_23_00"
    t1 = parse("2016-12-15T19:58:25Z")
    t2 = parse("2016-12-15T20:01:05Z")
    t1 = parse("2016-12-15T22:58:25Z")
    t2 = parse("2016-12-15T23:01:05Z")
    t1 = parse("2017-02-24T08:01:00Z")
    t2 = parse("2017-02-24T08:04:00Z")

    t_1_2 = TimeInterval(start=t1,end=t2)
    # w.factors[0].execute(t_1_2)
    w.execute(t_1_2)

    env_results = w.factors[0].tool.global_result_list

    csv_string = pd.DataFrame(env_results).to_csv(sep="\t", header=False)

    with open("mk/visualise_summaries/env_summaries_{}.csv".format(day_str), "w") as text_file:
        text_file.write(csv_string)

    # print(env_results)
    # print(percentile_results)

#    time_interval = TimeInterval.now_minus(minutes=1)
#    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.logging_parser(default_loglevel=logging.INFO)
    run(delete_existing_workflows=True, loglevel=args.loglevel)

