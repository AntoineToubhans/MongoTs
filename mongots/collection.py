from mongots.aggregateby import parse_aggregateby
from mongots.dataframe import build_dataframe
from mongots.insert import build_empty_document
from mongots.insert import build_filter
from mongots.insert import build_update
from mongots.query import build_initial_match
from mongots.query import build_project
from mongots.query import build_sort
from mongots.query import build_unwind_and_match


class MongoTSCollection():
    def __init__(self, mongo_collection):
        self._collection = mongo_collection

    def insert_one(self, value, timestamp, tags=None):
        """Insert one timestamped value into the MongoDb collection.

        Args:
            value (float): the value to be inserted
            timestamp (datetime): the timestamp for the value
            tags (dict, default=None):
                tags for the value;
                can be use the search/filter the value later.

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

    def query(self, start, end, tags=None, aggregateby=None, groupby=None):
        """Query the MongoDb database for various statistics about values
        after `start` and before `end` timestamps.
        Available statistics are: count / mean / std / min / max.

        Args:
            start (datetime): filters values after the start datetime
            end (datetime): filters values before the end datetime
            aggregateby (str):
                bucket statistics for each interval in the time range.
                Aggregateby options are:
                - '1y', '2y', ... : one year, two years, ...
                - '1m', '2m', ... : one month, two months, ...
                - '1d', '2d', ... : one day, two days, ...
                - '1h', '2h', ... : one hour, two hours, ...
            tags (dict, default=None):
                Filters the queried values.
                Similar to a filter when you do a find query in MongoDb.
            groupby (array): return statistics grouped by tags (string)

        Return (pandas.DataFrame):
            dataframe containing the statistics and indexed by datetimes
            and groupby tags (if any)
        """
        if aggregateby is None:
            raise NotImplementedError(
                'Queries without aggregation are not supported yet.',
            )
        parsed_aggregateby = parse_aggregateby(aggregateby)

        if groupby is None:
            groupby = []

        pipeline = []

        pipeline.append(build_initial_match(start, end, tags))
        pipeline.extend(build_unwind_and_match(start, end, parsed_aggregateby))
        pipeline.append(build_project(parsed_aggregateby, groupby))
        pipeline.append(build_sort())

        raw_data = list(self._collection.aggregate(pipeline))

        return build_dataframe(raw_data, parsed_aggregateby, groupby)
