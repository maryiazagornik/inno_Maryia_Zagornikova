# query_runner.py
from sql_queries import QUERIES

class QueryRunner:
    """
    Handles all business queries to the database.
    All calculations (age calculation, differences, aggregation)
    are performed on the database side.
    """

    SQL_QUERIES = QUERIES

    def __init__(self, connector: 'DatabaseConnector'):
        self.connector = connector

    def _format_results(self, records, column_names):
        """Helper method to convert (('Val1', 10),) to [{'col1': 'Val1', 'col2': 10}]."""
        if not records:
            return []
        return [dict(zip(column_names, row)) for row in records]

    def get_room_student_count(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES.get("room_student_count"),
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "student_count"])

    def get_top5_min_avg_age(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES.get("top5_min_avg_age"),
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "avg_age"])

    def get_top5_max_age_diff(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES.get("top5_max_age_diff"),
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "age_difference"])

    def get_mixed_gender_rooms(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES.get("mixed_gender_rooms"),
            fetch_many=True
        )
        return self._format_results(records, ["room_name"])

    def run_all_queries(self):
        """Collects results of all queries into one dictionary."""
        return {
            "task_1_room_student_count": self.get_room_student_count(),
            "task_2_top5_min_avg_age": self.get_top5_min_avg_age(),
            "task_3_top5_max_age_diff": self.get_top5_max_age_diff(),
            "task_4_mixed_gender_rooms": self.get_mixed_gender_rooms(),
        }