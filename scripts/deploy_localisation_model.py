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
from datetime import timedelta

globs = {
    'house': 1,
    'wearables': 'ABCD'
}


def run(house, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from workflows.asset_splitter import create_asset_splitter
    from workflows.deploy_localisation_model import create_workflow_localisation_predict

    hyperstream = HyperStream(loglevel=loglevel)
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    experiment_ids = A.find_streams(name="experiments_selected").values()[0].window(
        TimeInterval.up_to_now()).last().value

    experiment_ids_str = '_'.join(experiment_ids)
    workflow_id0 = "asset_splitter"
    workflow_id1 = "lda_localisation_model_predict_"+experiment_ids_str

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id0)
        hyperstream.workflow_manager.delete_workflow(workflow_id1)

    try:
        w0 = hyperstream.workflow_manager.workflows[workflow_id0]
    except KeyError:
        w0 = create_asset_splitter(hyperstream, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id0)

    try:
        w1 = hyperstream.workflow_manager.workflows[workflow_id1]
    except KeyError:
        w1 = create_workflow_localisation_predict(hyperstream, house=house, experiment_ids=experiment_ids, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id1)

    ti0 = TimeInterval.up_to_now()
    # ti1 = TimeInterval.now_minus(minutes=1)
    ti1 = TimeInterval(start=ti0.end - timedelta(minutes=1), end=ti0.end)
    w0.execute(ti0)
    w1.execute(ti1)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))

    from display_localisation_predictions import display_predictions
    display_predictions(hyperstream, ti1, house, wearables=globs['wearables'])


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    run(globs['house'])
