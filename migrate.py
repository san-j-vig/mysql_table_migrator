from config import MIGRATE_ALL, TABLES_TO_MIGRATE, TABLES_TO_SKIP, SOURCE_DB, TARGET_DB
from mysql import get_engine
import pandas as pd
import traceback
from logger import logger as log


class Migrate:

    def __init__(self):
        self.source_db = get_engine(SOURCE_DB)
        self.target_db = get_engine(TARGET_DB)

    def get_source_table_info(self):
        where = ''

        if not MIGRATE_ALL:
            where += f' AND table_name IN {TABLES_TO_MIGRATE}'.replace(',)', ')')

        if TABLES_TO_SKIP:
            where += f' AND table_name NOT IN {TABLES_TO_MIGRATE}'.replace(',)', ')')

        query = f'SELECT table_name FROM information_schema.TABLES WHERE ' \
                f'table_schema = "{SOURCE_DB.get("SCHEMA")}" ' \
                f'{where}'

        df = pd.read_sql(query, self.source_db)

        if not df.empty:
            return df['table_name'].to_list()
        else:
            return []

    def create_tables_in_target(self, table_name):
        if table_name:
            table = f'{SOURCE_DB.get("SCHEMA")}.{table_name}'
            create_query = pd.read_sql(f'SHOW CREATE TABLE {table}', self.source_db)

            if TARGET_DB.get('DROP_AND_CREATE'):
                try:
                    log.debug(f'Dropping table {table}...')
                    self.target_db.execute(f'DROP TABLE {table}')
                    log.debug(f'Dropped table {table}!')
                except Exception as e:
                    log.error(f'Unable to drop {table} \n {str(e)}')

                log.debug(f'Creating table {table}...')
                self.target_db.execute(create_query.iloc[0]["Create Table"])
                log.debug(f'Created table {table}!')

    def insert_to_target(self, table_name):
        if table_name:
            table = f'{SOURCE_DB.get("SCHEMA")}.{table_name}'
            log.debug(f'Inserting data {table}...')

            start_value = 0
            increment_count = 1000

            if TARGET_DB.get('TRUNCATE_TABLES'):
                log.debug(f'Truncating table {table}...')
                self.target_db.execute(f'TRUNCATE TABLE {table}')
                log.debug(f'Truncated table {table}!')

            total_count = pd.read_sql(f'SELECT COUNT(*) AS TOTAL FROM {table}', self.source_db).iloc[0]['TOTAL']

            while start_value < total_count:
                partial_data = pd.read_sql(f'SELECT * FROM {table} LIMIT {increment_count} OFFSET {start_value}',
                                           self.source_db)

                partial_data.to_sql(table_name, con=self.target_db, schema=SOURCE_DB.get("SCHEMA"), index=False,
                                    if_exists="append")

                start_value += increment_count

            log.debug(f'Insert Complete {table}!\n')


if __name__ == "__main__":
    obj = Migrate()
    table_names = obj.get_source_table_info()
    for table_name in table_names:
        try:
            obj.create_tables_in_target(table_name)
            obj.insert_to_target(table_name)
        except:
            log.error(traceback.format_exc())
