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
import logging
import sys
from os import path


def run(house, loglevel=logging.INFO):
    from hyperstream import HyperStream
    from workflows.asset_splitter import split_sphere_assets

    hyperstream = HyperStream(loglevel=loglevel, file_logger=None)
    split_sphere_assets(hyperstream, house=house)


if __name__ == '__main__':
    sys.path.insert(0, path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.wearable_list_parser(default_loglevel=logging.INFO)

    delete_existing_workflows = True

    run(args.house, args.loglevel)
