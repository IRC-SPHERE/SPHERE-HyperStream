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


def create_asset_splitter(hyperstream, safe=True):
    from hyperstream import TimeInterval

    workflow_id = "asset_splitter"

    D = hyperstream.channel_manager.mongo
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.sphere_assets

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Asset Splitter",
            owner="TD",
            description="Splits the assets into separate streams on sub-plates",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (
        ("devices",                                 A, []),
        ("devices_by_house",                        M, ["H"]),
        ("wearables_by_house",                      M, ["H.W"]),
        ("access_points_by_location",               M, ["H.L"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    # First create the plate values for the node
    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element="houses")
        ),
        source=N["devices"],
        output_plate=dict(
            plate_id="H",
            meta_data_id="house",
            description="All houses",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    ).execute(TimeInterval.up_to_now())

    # Now populate the node
    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_splitter",
            parameters=dict(element="houses")
        ),
        source=N["devices"],
        splitting_node=None,
        sink=N["devices_by_house"]
    ).execute(TimeInterval.up_to_now())

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="asset_plate_generator",
            parameters=dict(element="locations")
        ),
        source=N["devices_by_house"],
        output_plate=dict(
            plate_id="H.L",
            meta_data_id="location",
            description="All devices in all houses",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    ).execute(TimeInterval.up_to_now())

    # def func(instance):
    #     return instance
    #
    # w.create_node_creation_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="meta_instance",
    #         parameters=dict(func=func)
    #     ),
    #     source=N["experiments_list"],
    #     output_plate=dict(
    #         plate_id="H.LocalisationExperiment",
    #         meta_data_id="localisation-experiment",
    #         description="Technician localisation walk-around",
    #         use_provided_values=False
    #     ),
    #     plate_manager=hyperstream.plate_manager
    # )

    return w


if __name__ == '__main__':
    from hyperstream import HyperStream
    hyperstream = HyperStream()
    create_asset_splitter(hyperstream)
