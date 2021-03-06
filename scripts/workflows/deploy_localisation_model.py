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


def create_workflow_localisation_predict(hyperstream, house, experiment_ids, safe=True):
    experiment_ids_str = '_'.join(experiment_ids)
    workflow_id = "lda_localisation_model_predict_" + experiment_ids_str

    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    houses = hyperstream.plate_manager.plates["H"]
    wearables = hyperstream.plate_manager.plates["H.W"]
    models = hyperstream.plate_manager.plates["LocalisationModels"]


    with hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Live Predictions",
            owner="TD",
            description="Deploy the LDA localisation model for live predictions",
            online=True,
            safe=safe) as w:

        nodes = (
            ("rss_raw",                                 S, [houses]),
            ("location_prediction",                     D, [houses, models]),
            ("location_prediction_lda",                 M, [houses]),
            ("every_2s",                                M, [wearables]),
            ("rss_per_uid",                             M, [wearables]),
            ("rss_per_uid_2s",                          M, [wearables]),
            ("location_prediction_models_broadcasted",  M, [wearables]),
            ("predicted_locations_broadcasted",         D, [wearables]),
            ("wearables_by_house",                      A, [houses]),
            ("access_points_by_house",                  A, [houses])
        )

        # Create all of the nodes
        N = dict((stream_name, w.create_node(stream_name, channel, plates)) for stream_name, channel, plates in nodes)

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sphere",
                parameters=dict(modality="wearable", elements={"rss"})
            ),
            source=None,
            splitting_node=None,
            sink=N["rss_raw"])

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="splitter_from_stream",
                parameters=dict(
                    element="uid"
                )
            ),
            source=N["rss_raw"],
            splitting_node=N["wearables_by_house"],
            sink=N["rss_per_uid"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_window",
                parameters=dict(lower=-2.0, upper=0.0, increment=2.0)
            ),
            sources=None,
            sink=N["every_2s"])

        def component_wise_max(init_value=None, id_field='aid', value_field='wearable-rss'):
            if init_value is None:
                init_value = {}

            def func(data):
                result = init_value.copy()
                for (time, value) in data:
                    if value[id_field] in result:
                        result[value[id_field]] = max(result[value[id_field]], value[value_field])
                    else:
                        result[value[id_field]] = value[value_field]
                return result

            return func

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_apply",
                parameters=dict(func=component_wise_max())
            ),
            sources=[N["every_2s"], N["rss_per_uid"]],
            sink=N["rss_per_uid_2s"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="index_of",
                parameters=dict(selector_meta_data="localisation_model", index="lda")
            ),
            sources=[N['location_prediction']],
            sink=N["location_prediction_lda"])

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="stream_broadcaster_from_stream",
                parameters=dict(func=lambda x: x.last())
            ),
            source=N["location_prediction_lda"],
            splitting_node=N["wearables_by_house"],
            sink=N["location_prediction_models_broadcasted"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="localisation_model_predict",
                parameters=dict()
            ),
            sources=[N['location_prediction_models_broadcasted'], N["rss_per_uid_2s"]],
            sink=N["predicted_locations_broadcasted"])

        return w
