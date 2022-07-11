from urllib.parse import quote_plus
from sqlalchemy import create_engine


def get_engine(config):
    connection_string = "mysql+pymysql://{}:%s@{}:{}" \
                            .format(config.get('USERNAME'), config.get('HOST'), config.get('PORT')
                                    + '/' + config.get('SCHEMA')
                                    + '?charset=UTF8MB4') % quote_plus(config.get('PASSWORD'))

    return create_engine(connection_string,
                         pool_size=4, max_overflow=0, pool_use_lifo=True, pool_pre_ping=True, pool_recycle=3600,
                         isolation_level="AUTOCOMMIT")
