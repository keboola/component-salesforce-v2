import logging
import re
from enum import Enum
from typing import List, Callable


class QueryType(Enum):
    GET = "get"


class SoqlQuery:
    def __init__(self, query: str, sf_object: str, sf_object_fields: List[str], query_field_names: List[str],
                 query_type="get"):
        self.sf_object_fields = sf_object_fields
        self.sf_object = sf_object
        self.query_field_names = query_field_names
        self.query = query
        self.query_type = QueryType(query_type)
        self.check_query()

    @classmethod
    def build_from_object(cls, sf_object: str, describe_object_method: Callable[[str], List[str]],
                          query_type="get") -> 'SoqlQuery':
        sf_object_fields = describe_object_method(sf_object)
        query = cls._construct_soql_from_fields(sf_object, sf_object_fields)
        return SoqlQuery(query, sf_object, sf_object_fields, sf_object_fields, query_type)

    @classmethod
    def build_from_query_string(cls, query_string: str,
                                describe_object_method: Callable[[str], List[str]], query_type="get") -> 'SoqlQuery':
        sf_object = cls._get_object_from_query(query_string)
        sf_object_fields = describe_object_method(sf_object)
        query_field_names = cls._get_fields_from_query(query_string)
        return SoqlQuery(query_string, sf_object, sf_object_fields, query_field_names, query_type)

    @staticmethod
    def _list_to_lower(str_list):
        return [x.lower() for x in str_list]

    @staticmethod
    def _construct_soql_from_fields(sf_object, sf_object_fields):
        soql_query = f"SELECT {','.join(sf_object_fields)} FROM {sf_object}"
        return soql_query

    @staticmethod
    def _get_object_from_query(query):
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
            query_words = self.query.lower().split()
            if "select" not in query_words:
                raise ValueError("SOQL query must contain SELECT")
            if "from" not in query_words:
                raise ValueError("SOQL query must contain FROM")
            if "offset" in query_words:
                raise ValueError("SOQL bulk queries do not support OFFSET clauses")
            if "typeof" in query_words:
                raise ValueError("SOQL bulk queries do not support TYPEOF clauses")

    def set_query_to_incremental(self, incremental_field, continue_from_value):
        if incremental_field.lower() in self._list_to_lower(self.sf_object_fields):
            incremental_string = f" WHERE {incremental_field} >= {continue_from_value}"
        else:
            raise ValueError(f"Field {incremental_field} is not present in the {self.sf_object} object ")

        self.query = self._add_to_where_clause(self.query, incremental_string)

    def set_deleted_option_in_query(self, deleted):
        if not deleted and "isdeleted" in self._list_to_lower(self.sf_object_fields):
            is_deleted_string = " WHERE IsDeleted = false "
            self.query = self._add_to_where_clause(self.query, is_deleted_string)
        elif deleted and "isdeleted" not in self._list_to_lower(self.sf_object_fields):
            logging.warning(f"Waring: IsDeleted is not a field in the {self.sf_object} object, cannot fetch deleted "
                            f"records")

    @staticmethod
    def _add_to_where_clause(soql, new_where_string):
        and_string = " and "
        where_location_start = soql.lower().find(" where ")
        where_location_end = where_location_start + len(" where ")
        if where_location_start > 0:
            before_where = soql[:where_location_start]
            after_where = soql[where_location_end:]
            new_query = "".join([before_where, new_where_string, and_string, after_where])
        else:
            new_query = "".join([soql, new_where_string])
        return new_query

    def check_pkey_in_query(self, pkeys):
        missing_keys = []
        # split a string by space, comma, and period characters
        query_words = re.split("\\s|(?<!\\d)[,.](?!\\d)", self.query.lower())
        for pkey in pkeys:
            if pkey.lower() not in query_words:
                missing_keys.append(pkey)
        return missing_keys

    @staticmethod
    def _get_fields_from_query(query):
        fields = []

        # remove strings within brackets
        query_no_brackets = re.sub("\\(.*?\\)", "", query)
        fields.extend(SoqlQuery._get_fields_between_select_and_from(query_no_brackets))

        queries_in_brackets = re.findall("\\(.*?\\)", query)
        for query_in_brackets in queries_in_brackets:
            query_in_brackets = query_in_brackets.replace("(", "").replace(")", "")
            fields.extend(SoqlQuery._get_fields_between_select_and_from(query_in_brackets))

        return fields

    @staticmethod
    def _get_fields_between_select_and_from(query):
        # split by commma and space
        field_list = re.split('[, ]{1}[\\s]?', query)

        field_list_lower = SoqlQuery._list_to_lower(field_list)
        try:
            from_index = field_list_lower.index("from")
            select_index = field_list_lower.index("select")
        except ValueError as value_error:
            raise ValueError("SOQL Select Queries must contain both 'select' and 'from'") from value_error

        field_list = field_list[select_index + 1:from_index]
        field_list = [SoqlQuery._remove_object_from_field(field) for field in field_list]

        # remove non alphanumeric from fieldnames
        field_list = [re.sub(r'\W+', '', field) for field in field_list]
        return field_list

    @staticmethod
    def _remove_object_from_field(field):
        fields = field.split(".")
        return fields[-1]
