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

import numpy as np


def create_workflow_summaries_to_csv(hyperstream, safe=True):
    workflow_id = "summaries_to_csv"

#    S = hyperstream.channel_manager.sphere
#    D = hyperstream.channel_manager.mongo
    X = hyperstream.channel_manager.summary
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    with hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Summaries to csv",
            owner="MK",
            description="Save the periodic summaries to a csv file",
            online=True,
            safe=safe
            ) as w:

        nodes = (
            ("env_per_uid_field_agg_perc",              X, ["H.EnvSensors.Fields"]),
            ("env_per_uid_field_agg_hist",              X, ["H.EnvSensors.Fields"]),
            ("rss_per_uid_aid_value_agg_perc",          X, ["H.W","H.APs"]),
            ("rss_per_uid_aid_value_agg_hist",          X, ["H.W","H.APs"]),
            ("acc_per_uid_acclist_coord_agg_perc",      X, ["H.W.Coords3d"]),
            ("acc_per_uid_acclist_coord_agg_hist",      X, ["H.W.Coords3d"]),
            ("vid_per_uid_2dcen_x_perc",                X, ["H.Cameras"]),
            ("vid_per_uid_2dcen_x_hist",                X, ["H.Cameras"]),
            ("vid_per_uid_2dcen_y_perc",                X, ["H.Cameras"]),
            ("vid_per_uid_2dcen_y_hist",                X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_left_perc",              X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_left_hist",              X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_top_perc",               X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_top_hist",               X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_right_perc",             X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_right_hist",             X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_bottom_perc",            X, ["H.Cameras"]),
            ("vid_per_uid_2dbb_bottom_hist",            X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_left_perc",              X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_left_hist",              X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_top_perc",               X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_top_hist",               X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_front_perc",             X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_front_hist",             X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_right_perc",             X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_right_hist",             X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_bottom_perc",            X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_bottom_hist",            X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_back_perc",              X, ["H.Cameras"]),
            ("vid_per_uid_3dbb_back_hist",              X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_x_perc",                X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_x_hist",                X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_y_perc",                X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_y_hist",                X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_z_perc",                X, ["H.Cameras"]),
            ("vid_per_uid_3dcen_z_hist",                X, ["H.Cameras"]),
            ("vid_per_uid_activity_hist",               X, ["H.Cameras"]),
            ("vid_per_uid_intensity_hist",              X, ["H.Cameras"]),
            ("vid_per_uid_userid_hist",                 X, ["H.Cameras"]),
            ("prediction_mean",                         X, ["H.W"]),
            ("prediction_map_hist",                     X, ["H.W"]),
            ("env_sensors_by_house",                    A, ["H"]),
            ("fields_by_env_sensor",                    A, ["H.EnvSensors"]),
            ("cameras_by_house",                        A, ["H"]),
            ("wearables_by_house",                      A, ["H"]),
            ("access_points_by_house",                  A, ["H"]),
            ("percentiles_csv",                         M, ["H"])
        )

        # Create all of the nodes
        N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

        percentile_nodes = [
            "env_per_uid_field_agg_perc",
            "rss_per_uid_aid_value_agg_perc",
            "acc_per_uid_acclist_coord_agg_perc",
            "vid_per_uid_2dcen_x_perc",
            "vid_per_uid_2dcen_y_perc",
            "vid_per_uid_2dbb_left_perc",
            "vid_per_uid_2dbb_top_perc",
            "vid_per_uid_2dbb_right_perc",
            "vid_per_uid_2dbb_bottom_perc",
            "vid_per_uid_3dbb_left_perc",
            "vid_per_uid_3dbb_top_perc",
            "vid_per_uid_3dbb_front_perc",
            "vid_per_uid_3dbb_right_perc",
            "vid_per_uid_3dbb_bottom_perc",
            "vid_per_uid_3dbb_back_perc",
            "vid_per_uid_3dcen_x_perc",
            "vid_per_uid_3dcen_y_perc",
            "vid_per_uid_3dcen_z_perc"
        ]

        numeric_hist_nodes = [
            "env_per_uid_field_agg_hist",
            "rss_per_uid_aid_value_agg_hist",
            "acc_per_uid_acclist_coord_agg_hist",
        ]
        video_numeric_hist_nodes = [
            "vid_per_uid_2dcen_x_hist",
            "vid_per_uid_2dcen_y_hist",
            "vid_per_uid_2dbb_left_hist",
            "vid_per_uid_2dbb_top_hist",
            "vid_per_uid_2dbb_right_hist",
            "vid_per_uid_2dbb_bottom_hist",
            "vid_per_uid_3dbb_left_hist",
            "vid_per_uid_3dbb_top_hist",
            "vid_per_uid_3dbb_front_hist",
            "vid_per_uid_3dbb_right_hist",
            "vid_per_uid_3dbb_bottom_hist",
            "vid_per_uid_3dbb_back_hist",
            "vid_per_uid_3dcen_x_hist",
            "vid_per_uid_3dcen_y_hist",
            "vid_per_uid_3dcen_z_hist"
        ]

        categorical_hist_nodes = [
            "vid_per_uid_activity_hist",
            "vid_per_uid_intensity_hist",
            "vid_per_uid_userid_hist",
            "prediction_map_hist"
        ]

        more_nodes = ["prediction_mean"]

        n_segments = 4
        percentiles = [i/4 for i in range(n_segments+1)]

        def calc_bins(first_break,break_width,n_breaks,breaks):
            if breaks is None:
                breaks = [first_break + i * break_width for i in range(n_breaks)]
            breaks = [-np.inf] + breaks + [np.inf]
            return breaks

        binnings = dict(
            humidity   =calc_bins(first_break=0,break_width=1,n_breaks=101,breaks=None),
            pressure   =calc_bins(first_break=950,break_width=1,n_breaks=101,breaks=None),
            temperature=calc_bins(first_break=0,break_width=0.5,n_breaks=101,breaks=None),
            water      =calc_bins(first_break=0,break_width=1,n_breaks=11,breaks=None),
            light      =calc_bins(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(-5,15)]),
            motion     =calc_bins(first_break=0,break_width=1,n_breaks=2 ,breaks=None),
            electricity=calc_bins(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(15)]),
            rss        =calc_bins(first_break=-120,break_width=1,n_breaks=101,breaks=None),
            acc        =calc_bins(first_break=-5,break_width=0.1,n_breaks=101,breaks=None),
            vid        =calc_bins(first_break=-5000, break_width=100, n_breaks=101, breaks=None)
        )

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="percentiles_to_csv",
                parameters=dict(selector_meta_data=None)
                # parameters=dict(n_segments=4,percentiles=None)
            ),
            sources=[N[x] for x in percentile_nodes],
            sink=N["percentiles_csv"])


        return w

