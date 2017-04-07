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


# noinspection PyPep8Naming
def create_asset_splitter_0(hyperstream, house, safe=True):
    workflow_id = "asset_splitter_0"

    A = hyperstream.channel_manager.assets
    SA = hyperstream.channel_manager.sphere_assets

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Asset Splitter 0",
            owner="MK",
            description="Create the house plate (level 0 in the hierarchy)",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    filters = (house,) if house else None

    # First create the plate values for the node
    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element="house", filters=filters, use_value_instead_of_key=False)
        ),
        source=w.create_node("devices", SA, []),
        output_plate=dict(
            plate_id="H",
            meta_data_id="house",
            description="All houses",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    A.purge_node("devices")

    return w


def create_hypercat_parser(hyperstream, house, safe=True):
    from hyperstream.time_interval import TimeInterval, MIN_DATE

    SA = hyperstream.channel_manager.sphere_assets
    S = hyperstream.channel_manager.sphere

    workflow_id = "hypercat_reader"
    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="HyperCat Reader",
            owner="MK",
            description="Read the hypercat, and dump into the assets stream",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (("devices", SA, []),
             ("hc_devices", S, ["H"])
             )

    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    time_interval = TimeInterval(MIN_DATE, SA.up_to_timestamp)
    # time_interval = TimeInterval.up_to_now()

    w.create_multi_output_factor(tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="hypercat")),
        source=None,
        splitting_node=None,
        sink=N["hc_devices"])

    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(name="hypercat_parser", parameters=dict()),
    #     sources=[N["hc_devices"]],
    #     sink=N["devices"]
    # )

    w.execute(time_interval)

    source = N["hc_devices"].streams.values()[0]
    sink = N["devices"].streams.values()[0]
    ci = sink.calculated_intervals
    sink.calculated_intervals = []

    tool = hyperstream.channel_manager.get_tool(name="hypercat_parser", parameters=dict(house=house))
    tool.execute(sources=[source, sink], sink=sink, alignment_stream=None, interval=time_interval)

    sink.calculated_intervals = ci


# noinspection PyPep8Naming
def create_asset_splitter_1(hyperstream, house, safe=True):
    workflow_id = "asset_splitter_1"

    SA = hyperstream.channel_manager.sphere_assets
    A = hyperstream.channel_manager.assets

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Asset Splitter 1",
            owner="MK",
            description="Splits the assets into separate streams on house plate and creates sub-plates "
                        "(level 1 in the hierarchy)",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (
        ("devices",                                 SA, []),
        ("devices_by_house",                        A, ["H"]),
        ("wearables_by_house",                      A, ["H"]),
        ("access_points_by_house",                  A, ["H"]),
        ("cameras_by_house",                        A, ["H"]),
        ("env_sensors_by_house",                    A, ["H"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    A.purge_node("devices_by_house")
    A.purge_node("wearables_by_house")
    A.purge_node("access_points_by_house")
    A.purge_node("cameras_by_house")
    A.purge_node("env_sensors_by_house")

    # Now populate the node
    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_splitter",
            parameters=dict(element="house", filters=house)
        ),
        source=N["devices"],
        splitting_node=None,
        sink=N["devices_by_house"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key="env_sensors")
        ),
        sources=[N["devices_by_house"]],
        alignment_node=None,
        sink=N["env_sensors_by_house"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key="access_points")
        ),
        sources=[N["devices_by_house"]],
        alignment_node=None,
        sink=N["access_points_by_house"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key="wearables")
        ),
        sources=[N["devices_by_house"]],
        alignment_node=None,
        sink=N["wearables_by_house"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key="cameras")
        ),
        sources=[N["devices_by_house"]],
        alignment_node=None,
        sink=N["cameras_by_house"]
    )

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element=None, use_value_instead_of_key=False)
        ),
        source=N["env_sensors_by_house"],
        output_plate=dict(
            plate_id="H.EnvSensors",
            meta_data_id="env_sensor",
            description="All environmental sensors in each house",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element=None, use_value_instead_of_key=True)
        ),
        source=N["access_points_by_house"],
        output_plate=dict(
            plate_id="H.APs",
            meta_data_id="access_point",
            description="All access points in each house",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element=None, use_value_instead_of_key=True)
        ),
        source=N["wearables_by_house"],
        output_plate=dict(
            plate_id="H.W",
            meta_data_id="wearable",
            description="All wearables in each house",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element=None, use_value_instead_of_key=True)
        ),
        source=N["cameras_by_house"],
        output_plate=dict(
            plate_id="H.Cameras",
            meta_data_id="camera",
            description="All cameras in each house",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    return w


# noinspection PyPep8Naming
def create_asset_splitter_2(hyperstream, safe=True):
    workflow_id = "asset_splitter_2"

    A = hyperstream.channel_manager.assets

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Asset Splitter 2",
            owner="MK",
            description="Creates the sub-sub-plates (level 2 in the hierarchy)",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (
        ("env_sensors_by_house",                    A, ["H"]),
        ("fields_by_env_sensor",                    A, ["H.EnvSensors"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    A.purge_node("fields_by_env_sensor")

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_splitter",
            parameters=dict()
        ),
        source=N["env_sensors_by_house"],
        splitting_node=None,
        sink=N["fields_by_env_sensor"]
    )

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element=None, use_value_instead_of_key=False)
        ),
        source=N["fields_by_env_sensor"],
        output_plate=dict(
            plate_id="H.EnvSensors.Fields",
            meta_data_id="env_field",
            description="All fields in each environmental sensor",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    return w


def split_sphere_assets(hyperstream, house, delete_existing_workflows=True):
    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow("sphere_asset_splitter_0")
        hyperstream.workflow_manager.delete_workflow("sphere_asset_splitter_1")
        hyperstream.workflow_manager.delete_workflow("sphere_asset_splitter_2")

    from hyperstream import TimeInterval
    time_interval = TimeInterval.up_to_now()

    create_asset_splitter_0(hyperstream, house=house).execute(time_interval)
    create_hypercat_parser(hyperstream, house=house)
    create_asset_splitter_1(hyperstream, house=house).execute(time_interval)
    create_asset_splitter_2(hyperstream).execute(time_interval)
