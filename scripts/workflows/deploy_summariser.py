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


def create_workflow_coord_plate_creation(hyperstream, safe=True):
    workflow_id = "coord3d_plate_creation"

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Coord3d plate creation",
            owner="MK",
            description="Coord3d plate creation",
            online=True)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="meta_instance_from_list",
            parameters=dict(values=['x','y','z'])
        ),
        source=None,
        output_plate=dict(
            plate_id="H.W.Coords3d",
            meta_data_id="coord",
            description="3d coordinates x y z",
            use_provided_values=False,
            parent_plate="H.W"
        ),
        plate_manager=hyperstream.plate_manager
    )

    return w


def create_workflow_summariser(hyperstream,
                               env_window_size =1*60*60.0,
                               rss_window_size =4*60*60.0,
                               acc_window_size =4*60*60.0,
                               vid_window_size =4*60*60.0,
                               pred_window_size=4*60*60.0,
                               safe=True):

    workflow_id = "periodic_summaries"

    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Live Summaries",
            owner="MK",
            description="Live summaries of all modalities",
            online=True)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (
        ("env_raw",                                 S, ["H"]),
        ("env_per_uid",                             S, ["H.EnvSensors"]),
        ("env_per_uid_field",                       S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_windows",               M, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg",                   S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_perc",              D, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_hist",              D, ["H.EnvSensors.Fields"]),
        ("rss_raw",                                 S, ["H"]),
        ("rss_per_uid",                             S, ["H.W"]),
        ("rss_per_uid_aid",                         S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value",                   S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_windows",           M, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg",               S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg_perc",          D, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg_hist",          D, ["H.W","H.APs"]),
        ("acc_raw",                                 S, ["H"]),
        ("acc_per_uid",                             S, ["H.W"]),
        ("acc_per_uid_acclist",                     S, ["H.W"]),
        ("acc_per_uid_acclist_coord",               S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_windows",       M, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg",           S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg_perc",      D, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg_hist",      D, ["H.W.Coords3d"]),
        ("vid_raw",                                 S, ["H"]),
        ("vid_per_uid",                             S, ["H.Cameras"]),
        ("vid_per_uid_windows",                     M, ["H.Cameras"]),
        ("vid_per_uid_2dcen",                       S, ["H.Cameras"]),
        ("vid_per_uid_2dcen_x",                     S, ["H.Cameras"]),
        ("vid_per_uid_2dcen_x_agg",                 S, ["H.Cameras"]),
        ("vid_per_uid_2dcen_x_perc",                D, ["H.Cameras"]),
        ("vid_per_uid_2dcen_x_hist",                D, ["H.Cameras"]),
        ("vid_per_uid_2dcen_y",                     S, ["H.Cameras"]),
        ("vid_per_uid_2dcen_y_agg",                 S, ["H.Cameras"]),
        ("vid_per_uid_2dcen_y_perc",                D, ["H.Cameras"]),
        ("vid_per_uid_2dcen_y_hist",                D, ["H.Cameras"]),
        ("vid_per_uid_2dbb",                        S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_left",                   S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_left_agg",               S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_left_perc",              D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_left_hist",              D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_top",                    S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_top_agg",                S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_top_perc",               D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_top_hist",               D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_right",                  S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_right_agg",              S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_right_perc",             D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_right_hist",             D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_bottom",                 S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_bottom_agg",             S, ["H.Cameras"]),
        ("vid_per_uid_2dbb_bottom_perc",            D, ["H.Cameras"]),
        ("vid_per_uid_2dbb_bottom_hist",            D, ["H.Cameras"]),
        ("vid_per_uid_3dbb",                        S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_left",                   S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_left_agg",               S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_left_perc",              D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_left_hist",              D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_top",                    S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_top_agg",                S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_top_perc",               D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_top_hist",               D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_front",                  S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_front_agg",              S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_front_perc",             D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_front_hist",             D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_right",                  S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_right_agg",              S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_right_perc",             D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_right_hist",             D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_bottom",                 S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_bottom_agg",             S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_bottom_perc",            D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_bottom_hist",            D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_back",                   S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_back_agg",               S, ["H.Cameras"]),
        ("vid_per_uid_3dbb_back_perc",              D, ["H.Cameras"]),
        ("vid_per_uid_3dbb_back_hist",              D, ["H.Cameras"]),
        ("vid_per_uid_3dcen",                       S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_x",                     S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_x_agg",                 S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_x_perc",                D, ["H.Cameras"]),
        ("vid_per_uid_3dcen_x_hist",                D, ["H.Cameras"]),
        ("vid_per_uid_3dcen_y",                     S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_y_agg",                 S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_y_perc",                D, ["H.Cameras"]),
        ("vid_per_uid_3dcen_y_hist",                D, ["H.Cameras"]),
        ("vid_per_uid_3dcen_z",                     S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_z_agg",                 S, ["H.Cameras"]),
        ("vid_per_uid_3dcen_z_perc",                D, ["H.Cameras"]),
        ("vid_per_uid_3dcen_z_hist",                D, ["H.Cameras"]),
        ("vid_per_uid_activity",                    S, ["H.Cameras"]),
        ("vid_per_uid_activity_agg",                S, ["H.Cameras"]),
        ("vid_per_uid_activity_hist",               D, ["H.Cameras"]),
        ("vid_per_uid_intensity",                   S, ["H.Cameras"]),
        ("vid_per_uid_intensity_agg",               S, ["H.Cameras"]),
        ("vid_per_uid_intensity_hist",              D, ["H.Cameras"]),
        ("vid_per_uid_userid",                      S, ["H.Cameras"]),
        ("vid_per_uid_userid_agg",                  S, ["H.Cameras"]),
        ("vid_per_uid_userid_hist",                 D, ["H.Cameras"]),
        ("prediction",                              S, ["H"]),
        ("prediction_windows",                      M, ["H"]),
        ("prediction_agg",                          S, ["H"]),
        ("prediction_mean",                         D, ["H"]),
        ("prediction_map",                          S, ["H"]),
        ("prediction_map_agg",                      S, ["H"]),
        ("prediction_map_hist",                     D, ["H"]),
        ("env_sensors_by_house",                    A, ["H"]),
        ("fields_by_env_sensor",                    A, ["H.EnvSensors"]),
        ("cameras_by_house",                        A, ["H"]),
        ("wearables_by_house",                      A, ["H"]),
        ("access_points_by_house",                  A, ["H"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    #############################
    ### ENVIRONMENTAL SENSORS ###
    #############################

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental")
        ),
        source=None,
        splitting_node=None,
        sink=N["env_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid",use_mapping_keys_only=True)
        ),
        source=N["env_raw"],
        splitting_node=N["env_sensors_by_house"],
        sink=N["env_per_uid"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict()
        ),
        source=N["env_per_uid"],
        splitting_node=N["fields_by_env_sensor"],
        sink=N["env_per_uid_field"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-env_window_size, upper=0.0, increment=env_window_size)
        ),
        sources=None,
        sink=N["env_per_uid_field_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["env_per_uid_field_windows"], N["env_per_uid_field"]],
        sink=N["env_per_uid_field_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="percentiles_from_list",
            parameters=dict(n_segments=4,percentiles=None)
        ),
        sources=[N["env_per_uid_field_agg"]],
        sink=N["env_per_uid_field_agg_perc"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sensor_field_histogram",
            parameters=dict(field_specific_params=dict(
                humidity   =dict(first_break=0,break_width=1,n_breaks=101,breaks=None),
                pressure   =dict(first_break=950,break_width=1,n_breaks=101,breaks=None),
                temperature=dict(first_break=0,break_width=0.5,n_breaks=101,breaks=None),
                water      =dict(first_break=0,break_width=1,n_breaks=11,breaks=None),
                light      =dict(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(-5,15)]),
                motion     =dict(first_break=0,break_width=1,n_breaks=2 ,breaks=None),
                electricity=dict(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(15)])
            ))
        ),
        sources=[N["env_per_uid_field_agg"]],
        sink=N["env_per_uid_field_agg_hist"])

    ############################
    ### WEARABLE SENSORS RSS ###
    ############################

    w.create_multi_output_factor(
       tool=hyperstream.channel_manager.get_tool(
           name="sphere",
           parameters=dict(modality="wearable",elements={"rss"})
       ),
       source=None,
       splitting_node=None,
       sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid")
        ),
        source=N["rss_raw"],
        splitting_node=N["wearables_by_house"],
        sink=N["rss_per_uid"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="aid")
        ),
        source=N["rss_per_uid"],
        splitting_node=N["access_points_by_house"],
        sink=N["rss_per_uid_aid"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key='wearable-rss')
        ),
        sources=[N["rss_per_uid_aid"]],
        sink=N["rss_per_uid_aid_value"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-rss_window_size, upper=0.0, increment=rss_window_size)
        ),
        sources=None,
        sink=N["rss_per_uid_aid_value_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["rss_per_uid_aid_value_windows"], N["rss_per_uid_aid_value"]],
        sink=N["rss_per_uid_aid_value_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="percentiles_from_list",
            parameters=dict(n_segments=4,percentiles=None)
        ),
        sources=[N["rss_per_uid_aid_value_agg"]],
        sink=N["rss_per_uid_aid_value_agg_perc"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="histogram_from_list",
            parameters=dict(first_break=-120,break_width=1,n_breaks=101,breaks=None)
        ),
        sources=[N["rss_per_uid_aid_value_agg"]],
        sink=N["rss_per_uid_aid_value_agg_hist"])

    #####################################
    ### WEARABLE SENSORS ACCELERATION ###
    #####################################

    w.create_multi_output_factor(
       tool=hyperstream.channel_manager.get_tool(
           name="sphere",
           parameters=dict(modality="wearable",elements={"xl"})
       ),
       source=None,
       splitting_node=None,
       sink=N["acc_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid")
        ),
        source=N["acc_raw"],
        splitting_node=N["wearables_by_house"],
        sink=N["acc_per_uid"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key='wearable-xl1')
        ),
        sources=[N["acc_per_uid"]],
        sink=N["acc_per_uid_acclist"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_of_list",
            parameters=dict(mapping=['x','y','z'])
        ),
        source=N["acc_per_uid_acclist"],
        splitting_node=None,
        sink=N["acc_per_uid_acclist_coord"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-acc_window_size, upper=0.0, increment=acc_window_size)
        ),
        sources=None,
        sink=N["acc_per_uid_acclist_coord_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["acc_per_uid_acclist_coord_windows"], N["acc_per_uid_acclist_coord"]],
        sink=N["acc_per_uid_acclist_coord_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="percentiles_from_list",
            parameters=dict(n_segments=4,percentiles=None)
        ),
        sources=[N["acc_per_uid_acclist_coord_agg"]],
        sink=N["acc_per_uid_acclist_coord_agg_perc"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="histogram_from_list",
            parameters=dict(first_break=-5,break_width=0.1,n_breaks=101,breaks=None)
        ),
        sources=[N["acc_per_uid_acclist_coord_agg"]],
        sink=N["acc_per_uid_acclist_coord_agg_hist"])

    #####################
    ### VIDEO SENSORS ###
    #####################

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="video")
        ),
        source=None,
        splitting_node=None,
        sink=N["vid_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid",use_mapping_keys_only=False)
        ),
        source=N["vid_raw"],
        splitting_node=N["cameras_by_house"],
        sink=N["vid_per_uid"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-vid_window_size, upper=0.0, increment=vid_window_size)
        ),
        sources=None,
        sink=N["vid_per_uid_windows"])

    structure = {
        '2dcen': ['x','y'],
        '2dbb' : ['left','top','right','bottom'],
        '3dbb' : ['left','top','front','right','bottom','back'],
        '3dcen': ['x','y','z']
    }
    comp_names = {
        '2dcen': 'video-2DCen',
        '2dbb' : 'video-2Dbb',
        '3dbb' : 'video-3Dbb',
        '3dcen': 'video-3Dcen',
        'activity' : 'video-Activity',
        'intensity': 'video-Intensity',
        'userid'   : 'video-userID'
    }
    create_histograms_for = ['activity','intensity','userid']

    for k in comp_names.keys():
        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="component",
                parameters=dict(key=comp_names[k])
            ),
            sources=[N["vid_per_uid"]],
            sink=N["vid_per_uid_"+k])

    for k in structure.keys():
        for i in range(len(structure[k])):
            ki = k+"_"+structure[k][i]
            w.create_factor(
                tool=hyperstream.channel_manager.get_tool(
                    name="slice",
                    parameters=dict(index=i)
                ),
                sources=[N["vid_per_uid_"+k]],
                sink=N["vid_per_uid_"+ki])

            w.create_factor(
                tool=hyperstream.channel_manager.get_tool(
                    name="sliding_listify",
                    parameters=dict()
                ),
                sources=[N["vid_per_uid_windows"], N["vid_per_uid_"+ki]],
                sink=N["vid_per_uid_"+ki+"_agg"])

            w.create_factor(
                tool=hyperstream.channel_manager.get_tool(
                    name="percentiles_from_list",
                    parameters=dict(n_segments=4,percentiles=None)
                ),
                sources=[N["vid_per_uid_"+ki+"_agg"]],
                sink=N["vid_per_uid_"+ki+"_perc"])

            w.create_factor(
                tool=hyperstream.channel_manager.get_tool(
                    name="histogram_from_list",
                    parameters=dict(first_break=-5000, break_width=100, n_breaks=101, breaks=None)
                ),
                sources=[N["vid_per_uid_"+ki+"_agg"]],
                sink=N["vid_per_uid_"+ki+"_hist"])

    for k in create_histograms_for:

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_listify",
                parameters=dict()
            ),
            sources=[N["vid_per_uid_windows"], N["vid_per_uid_"+k]],
            sink=N["vid_per_uid_"+k+"_agg"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="histogram_from_list",
                parameters=dict(categorical=True)
            ),
            sources=[N["vid_per_uid_"+k+"_agg"]],
            sink=N["vid_per_uid_"+k+"_hist"])

    ###################
    ### PREDICTIONS ###
    ###################

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="random_predictions",
            parameters=dict(locations=['kitchen','lounge','hall'])
        ),
        sources=[N["env_raw"]],
        sink=N["prediction"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-pred_window_size, upper=0.0, increment=pred_window_size)
        ),
        sources=None,
        sink=N["prediction_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["prediction_windows"], N["prediction"]],
        sink=N["prediction_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="list_dict_mean",
            parameters=dict()
        ),
        sources=[N["prediction_agg"]],
        sink=N["prediction_mean"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="dict_argmax",
            parameters=dict()
        ),
        sources=[N["prediction"]],
        sink=N["prediction_map"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["prediction_windows"], N["prediction_map"]],
        sink=N["prediction_map_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="histogram_from_list",
            parameters=dict(categorical=True)
        ),
        sources=[N["prediction_map_agg"]],
        sink=N["prediction_map_hist"])

    return w

