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


class ArgumentParser(object):
    CHOICES = (logging.DEBUG, logging.INFO, logging.WARN, logging.ERROR, logging.CRITICAL)

    @staticmethod
    def logging_parser(default_loglevel=logging.DEBUG):
        """
        Gets a default argument parser including the house id and logging level
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--loglevel", dest='loglevel', nargs='?', type=int, default=default_loglevel,
                            choices=ArgumentParser.CHOICES)
        return parser.parse_args()

    @staticmethod
    def house_parser(default_loglevel=logging.DEBUG):
        """
        Gets a default argument parser including the house id and logging level
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--house', dest='house')
        parser.add_argument("--loglevel", dest='loglevel', nargs='?', type=int, default=default_loglevel,
                            choices=ArgumentParser.CHOICES)
        return parser.parse_args()

    @staticmethod
    def technician_selection_parser(default_loglevel=logging.DEBUG):
        """
        Gets an argument parser including the house id, technician's selection, and logging level
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--house', dest='house')
        parser.add_argument('--selection', dest='technicians_selection', nargs=2, type=int)
        parser.add_argument("--loglevel", dest='loglevel', nargs='?', type=int, default=default_loglevel,
                            choices=ArgumentParser.CHOICES)
        return parser.parse_args()

    @staticmethod
    def wearable_list_parser(wearables="ABCDEF", default_loglevel=logging.DEBUG):
        """
        Gets an argument parser including the house id, list of wearables, and logging level
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--house', dest='house', help='current house id or "all" for all houses')
        parser.add_argument("--wearables", dest='wearables', default=wearables)
        parser.add_argument("--loglevel", dest='loglevel', nargs='?', type=int, default=default_loglevel,
                            choices=ArgumentParser.CHOICES)
        return parser.parse_args()


    @staticmethod
    def wearable_tap_sync_parser(default_loglevel=logging.DEBUG):
        """
        Gets an argument parser including the house id, approximate time of synchronising 5 taps and logging level
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--house', dest='house')
        parser.add_argument("--time", dest='time')
        parser.add_argument("--loglevel", dest='loglevel', nargs='?', type=int, default=default_loglevel,
                            choices=ArgumentParser.CHOICES)
        return parser.parse_args()
