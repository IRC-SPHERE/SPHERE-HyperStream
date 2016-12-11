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

import argparse
import logging

CHOICES = (logging.DEBUG, logging.INFO, logging.WARN, logging.ERROR, logging.CRITICAL)


def get_default_parser(default_loglevel=logging.DEBUG):
    """
    Gets a default argument parser including the house id and logging level
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("house")
    parser.add_argument("loglevel", nargs='?', type=int, default=default_loglevel, choices=CHOICES)
    return parser.parse_args()


def get_technician_selection_parser(default_loglevel=logging.DEBUG):
    """
    Gets an argument parser including the house id, technician's selection, and logging level
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("house")
    parser.add_argument("technicians_selection", nargs=2)
    parser.add_argument("loglevel", nargs='?', type=int, default=default_loglevel, choices=CHOICES)
    return parser.parse_args()


def get_wearable_list_parser(wearables="ABCDEF", default_loglevel=logging.DEBUG):
    """
    Gets an argument parser including the house id, list of wearables, and logging level
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("house")
    parser.add_argument("wearables", default=wearables)
    parser.add_argument("loglevel", nargs='?', type=int, default=default_loglevel, choices=CHOICES)
    return parser.parse_args()
