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
import sys
from pprint import pprint, pformat


def create_selected_localisation_plates(hyperstream):
    meta_data_id = "localisation-experiment"

    hyperstream.plate_manager.delete_plate("H.SelectedLocalisationExperiment")
    hyperstream.plate_manager.create_plate(
        plate_id="H.SelectedLocalisationExperiment",
        description="Localisation experiments selected by the technician in SPHERE house",
        meta_data_id=meta_data_id,
        values=[],
        complement=True,
        parent_plate="H"
    )


def run(house, selection, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, StreamId, TimeInterval
    from workflows.display_experiments import create_workflow_list_technicians_walkarounds
    from workflows.learn_localisation_model import create_workflow_lda_localisation_model_learner
    from hyperstream.utils import StreamNotFoundError, reconstruct_interval

    hyperstream = HyperStream(loglevel=loglevel, file_logger=None)
    M = hyperstream.channel_manager.memory
    D = hyperstream.channel_manager.mongo
    A = hyperstream.channel_manager.assets

    workflow_id0 = "list_technicians_walkarounds"

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id0)

    try:
        w0 = hyperstream.workflow_manager.workflows[workflow_id0]
    except KeyError:
        w0 = create_workflow_list_technicians_walkarounds(hyperstream, house=house, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id0)
    time_interval = TimeInterval.up_to_now()
    w0.execute(time_interval)

    # from datetime import timedelta
    # time_interval.end += timedelta(milliseconds=1)
    df = M[StreamId('experiments_dataframe', (('house', house),))].window().values()[0]
    experiment_ids = set([df['experiment_id'][i - 1] for i in selection])

    experiment_ids_str = '_'.join(experiment_ids)

    create_selected_localisation_plates(hyperstream)

    # Ensure the model is overwritten if it's already there
    for model_name in ('lda','svm','room_rssi_hmm'):
        model_id = StreamId(
            name="location_prediction",
            meta_data=(('house', house), ('localisation_model', model_name)))
        try:
            hyperstream.channel_manager.mongo.purge_stream(model_id)
        except StreamNotFoundError:
            pass

    workflow_id1 = "lda_localisation_model_learner_"+experiment_ids_str

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id1)

    try:
        w1 = hyperstream.workflow_manager.workflows[workflow_id1]
    except KeyError:
        w1 = create_workflow_lda_localisation_model_learner(
            hyperstream, house=house, experiment_ids=experiment_ids, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id1)

    # Put the experiments selected into an asset stream
    from hyperstream import StreamInstance
    from hyperstream.utils import utcnow

    A.write_to_stream(
        stream_id=StreamId(name="experiments_selected", meta_data=(('house', house),)),
        data=StreamInstance(timestamp=utcnow(), value=list(experiment_ids))
    )

    time_interval = TimeInterval.up_to_now()
    w1.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))

    for model_name in ('lda','svm','room_rssi_hmm'):
        print("Model: {}".format(model_name))
        model_id = StreamId(
            name="location_prediction",
            meta_data=(('house', house), ('localisation_model', model_name)))
        try:
            model = D[model_id].window().last().value
        except (AttributeError, KeyError):
            print("No {} model was learnt - not requested or no data recorded?".format(model_name))

        for experiment_id in list(experiment_ids):
            print("Experiment id: {}".format(experiment_id))
            print("Time interval: {}".format(reconstruct_interval(experiment_id)))
            print("Accuracy: {}".format(pformat(model['performance'][experiment_id]['accuracy'])))
            print("Macro F1: {}".format(pformat(model['performance'][experiment_id]['f1_score_macro'])))
            print("Micro F1: {}".format(pformat(model['performance'][experiment_id]['f1_score_micro'])))
            print("Confusion Matrix:")
            pprint(model['performance'][experiment_id]['confusion_matrix'])
            print("")
    return True


if __name__ == '__main__':
    from os import path
    sys.path.insert(0, path.dirname(path.dirname(path.abspath(__file__))))

    from sphere_plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.technician_selection_parser(default_loglevel=logging.INFO)
    run(house=args.house, selection=map(int, args.technicians_selection), loglevel=args.loglevel)
