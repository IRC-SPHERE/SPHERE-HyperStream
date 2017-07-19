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
from mongoengine.errors import NotUniqueError, InvalidDocumentError
from mongoengine.context_managers import switch_db
from pymongo.errors import InvalidDocument

from hyperstream.channels import DatabaseChannel
from hyperstream.models import StreamInstanceModel
from hyperstream import StreamInstance


class SummaryInstanceModel(StreamInstanceModel):
    meta = {
        'collection': 'summaries',
        'indexes': [
            {'fields': ['stream_id']},
            {'fields': ['stream_id', 'datetime'], 'unique': True}
        ],
        'ordering': ['datetime']
    }


class SummaryChannel(DatabaseChannel):
    """
    Channel for storing summaries
    """
    def get_results(self, stream, time_interval):
        """
        Get the results for a given stream
        :param time_interval: The time interval
        :param stream: The stream object
        :return: A generator over stream instances
        """
        query = stream.stream_id.as_raw()
        query['datetime'] = {'$gt': time_interval.start, '$lte': time_interval.end}
        with switch_db(SummaryInstanceModel, 'hyperstream'):
            for instance in SummaryInstanceModel.objects(__raw__=query):
                yield StreamInstance(timestamp=instance.datetime, value=instance.value)

    def get_stream_writer(self, stream):
        """
        Gets the database channel writer
        The mongoengine model checks whether a stream_id/datetime pair already exists in the DB (unique pairs)
        Should be overridden by users' personal channels - allows for non-mongo outputs.
        :param stream: The stream
        :return: The stream writer function
        """

        def writer(document_collection):
            with switch_db(SummaryInstanceModel, 'hyperstream'):
                if isinstance(document_collection, StreamInstance):
                    document_collection = [document_collection]

                for t, doc in document_collection:
                    instance = SummaryInstanceModel(
                        stream_id=stream.stream_id.as_dict(),
                        datetime=t,
                        value=doc)
                    try:
                        instance.save()
                    except NotUniqueError as e:
                        # Implies that this has already been written to the database
                        # Raise an error if the value differs from that in the database
                        logging.warn("Found duplicate document: {}".format(e.message))
                        existing = SummaryInstanceModel.objects(stream_id=stream.stream_id.as_dict(), datetime=t)[0]
                        if existing.value != doc:
                            raise e
                    except (InvalidDocumentError, InvalidDocument) as e:
                        # Something wrong with the document - log the error
                        logging.error(e)
        return writer
