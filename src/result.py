import csv

from keboola.component.dao import TableDefinition


class Writer:

    def __init__(self, table_definition: TableDefinition):

        self.name = table_definition.name
        self.full_path = table_definition.full_path
        self.pk = table_definition.primary_key
        self.incremental = table_definition.incremental

    def __enter__(self):
        self.io = None
        self.writer = None

        return self

    def __exit__(self, type, value, traceback):

        if self.io is not None:
            self.io.close()
            self.io = None

    def create_writer(self, columns, headerless=True):
        self.io = open(self.full_path, 'w')
        self.writer = csv.DictWriter(self.io, fieldnames=columns, restval='',
                                     extrasaction='ignore', quoting=csv.QUOTE_ALL)

        if headerless is False:
            self.writer.writeheader()

    def write_first_row(self, row):
        self.writer.writerow(row)

    def write_results_with_reader(self, rows):
        for row in rows:
            self.writer.writerow(row)
