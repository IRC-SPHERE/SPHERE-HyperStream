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


def create_workflow_localisation_predict(hs, house, experiment_ids, safe=True):
    experiment_ids_str = '_'.join(experiment_ids)
    workflow_id = "lda_localisation_model_predict_" + experiment_ids_str

    sp = hs.plugins.sphere

    S = hs.channel_manager.sphere
    D = hs.channel_manager.mongo
    M = hs.channel_manager.memory
    A = hs.channel_manager.assets

    houses = hs.plate_manager.plates["H"]
    wearables = hs.plate_manager.plates["H.W"]
    models = hs.plate_manager.plates["LocalisationModels"]

    with hs.create_workflow(
            workflow_id=workflow_id,
            name="Live Predictions",
            owner="TD",
            description="Deploy the LDA localisation model for live predictions",
            online=True,
            safe=safe) as w:

        nodes = (
            ("rss_raw",                                 S, ["H"]),
            ("location_prediction",                     D, ["H", "LocalisationModels"]),
            ("location_prediction_lda",                 M, ["H"]),
            ("every_2s",                                M, ["H.W"]),
            ("rss_per_uid",                             M, ["H.W"]),
            ("rss_per_uid_2s",                          M, ["H.W"]),
            ("location_prediction_models_broadcasted",  M, ["H.W"]),
            ("predicted_locations_broadcasted",         D, ["H.W"]),
            ("wearables_by_house",                      A, ["H"]),
            ("access_points_by_house",                  A, ["H"])
        )

        # Create all of the nodes
        N = dict((stream_name, w.create_node(stream_name, channel, plate_ids))
                 for stream_name, channel, plate_ids in nodes)

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

        for house in houses:
            N["rss_raw"][house] = sp.factors.sphere(source=None, modality="wearable", elements={"rss"})

            for wearable in wearables[house]:
                N["rss_per_uid"][house][wearable] = hs.factors.splitter_from_stream(
                    source=N["rss_raw"], splitting_node=N["wearables_by_house"], element="uid")

                N["every_2s"][house][wearable] = hs.factors.sliding_window(
                    sources=None, lower=-2.0, upper=0.0, increment=2.0)

                N["rss_per_uid_2s"][house][wearable] = hs.factors.sliding_apply(
                    sources=[N["every_2s"], N["rss_per_uid"]], func=component_wise_max())

            N["location_prediction_lda"][house] = hs.factors.index_of(
                sources=[N["location_prediction"]], selector_meta_data="localisation_model", index="lda")

            for model in models:
                N["location_prediction_models_broadcasted"][house, model] = hs.factors.stream_broadcaster_from_stream(
                    source=N["location_prediction_lda"], func=lambda x: x.last())

            for wearable in wearables:
                N["predicted_locations_broadcasted"][house][wearable] = sp.factors.localisation_model_predict(
                    sources=[N['location_prediction_models_broadcasted'], N["rss_per_uid_2s"]])

        return w
