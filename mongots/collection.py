import pandas as pd

from mongots.insert import build_empty_document
from mongots.insert import build_filter
from mongots.insert import build_update
from mongots.query import build_initial_match
from mongots.query import build_unwind_and_match


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, timestamp, tags=None):
        """Insert one timestamped value into the MongoDb collection.

        Args:
            value (float): the value to be inserted
            timestamp (datetime): the timestamp for the value
            tags (dict, default=None): tags for the value; can be use the search/filter the value later on.

        Return (bool): True if the insertion succeeded, False otherwise.
        """
        filters = build_filter(timestamp, tags)
        update = build_update(value, timestamp)

        result = self._collection.update_one(filters, update, upsert=False)

        if result.modified_count == 0:
            empty_document = build_empty_document(timestamp)
            empty_document.update(filters)

            self._collection.insert_one(empty_document)

            result = self._collection.update_one(filters, update, upsert=False)

        return 1 == result.modified_count

    def query(self, start, end, interval=None, tags=None, groupby=None):
        """Query the MongoDb collections and returns various statistics (count, mean, std) for each
        interval between the start and the end datetime.

        Args:
            start (datetime): filters values after the start datetime
            end (datetime): filters values before the end datetime
            interval (str): compute statistics for each year ('1y'), month ('1m'), day ('1d') or hour ('1h')
            tags (dict, default=None): similar to a mongo filter
            groupby (array): return statistics grouped by a list of tags (string)

        Return (pandas.DataFrame): dataframe containing the statistics and indexed by datetimes.
        """
        pipeline = []

        pipeline.append(build_initial_match(start, end, tags))
        pipeline.extend(build_unwind_and_match(start, end, interval))

        raw_result = list(self._collection.aggregate(pipeline))

        return pd.DataFrame(raw_result)
