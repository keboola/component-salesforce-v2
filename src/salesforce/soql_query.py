from enum import Enum
import re
import logging


class QueryType(Enum):
    GET = "get"


class SoqlQuery:
    def __init__(self, query=None, sf_object=None, sf_object_fields=None, query_type="get"):
        if sf_object_fields:
            self.sf_object_fields = self.list_to_lower(sf_object_fields)
        self.sf_object = sf_object if sf_object else self.get_object_from_query(query)
        self.query = query if query else self.construct_soql_from_fields()
        self.query_type = QueryType(query_type)
        self.check_query()

    def set_sf_object_fields(self, sf_object_fields):
        self.sf_object_fields = self.list_to_lower(sf_object_fields)

    @staticmethod
    def list_to_lower(str_list):
        return [x.lower() for x in str_list]

    def construct_soql_from_fields(self):
        soql_query = f"SELECT {','.join(self.sf_object_fields)} FROM {self.sf_object}"
        return soql_query

    @staticmethod
    def get_object_from_query(query):
        # remove strings within brackets
        query_no_brackets = re.sub("\\(.*?\\)", "", query)
        # list words in query
        word_list = query_no_brackets.lower().split()
        # Only 1 from should exist
        from_index = word_list.index("from")
        #  object name is 1 word after the "from"
        object_name = word_list[from_index + 1]
        # remove non alphanumeric from objectname
        object_name = re.sub(r'\W+', '', object_name)
        return object_name

    def check_query(self):
        if not isinstance(self.query, str):
            raise ValueError("SOQL query must be a single string")
        if self.query_type == QueryType.GET:
            if "select" not in self.query.lower():
                raise ValueError("SOQL query must contain SELECT")
            if "from" not in self.query.lower():
                raise ValueError("SOQL query must contain FROM")

    def set_query_to_incremental(self, incremental_field, last_state):
        if incremental_field.lower() in self.sf_object_fields:
            incremental_string = f" WHERE {incremental_field} >= {last_state}"
        else:
            raise ValueError(f"Field {incremental_field} is not present in the {self.sf_object} object ")

        self.query = self._add_to_where_clause(self.query, incremental_string)

    def set_deleted_option_in_query(self, deleted):
        if not deleted and "isdeleted" in self.sf_object_fields:
            is_deleted_string = " WHERE IsDeleted = false "
            self.query = self._add_to_where_clause(self.query, is_deleted_string)
        elif deleted and "isdeleted" not in self.sf_object_fields:
            logging.warning(f"Waring: IsDeleted is not a field in the {self.sf_object} object, cannot fetch deleted "
                            f"records")

    @staticmethod
    def _add_to_where_clause(soql, new_where_string):
        and_string = " and "
        where_location = soql.lower().find(" where ")
        if where_location > 0:
            before_where, after_where = soql.lower().split(" where ")
            new_query = "".join([before_where, new_where_string, and_string, after_where])
        else:
            new_query = "".join([soql, new_where_string])
        return new_query
