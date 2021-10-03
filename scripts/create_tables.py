import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_query
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def create_schema(session, db_name, arn):
    """Creates schemas as defined in the queries in the list create_schema_query in sql_queries

        Args:
                session: SQLAlchemy sessionmaker
                db_name (str): the name of the database
                arn (str): Amazon Resource Name 
    """
    for query in create_schema_query:
        logging.info('Starting query {}'.format(query))
        session.connection().connection.set_isolation_level(0)
        session.execute(query.format(db_name, arn))
        session.connection().connection.set_isolation_level(1)
        logging.info('Query completed')


def drop_tables(session):
    """Drops tables as defined in the queries in the list drop_table_queries in sql_queries

        Args:
                session: SQLAlchemy sessionmaker
    """
    for query in drop_table_queries:
        logging.info('Starting query {}'.format(query))
        session.connection().connection.set_isolation_level(0)
        session.execute(query)
        session.connection().connection.set_isolation_level(1)
        logging.info('Query completed')


def create_tables(session):
    """Creates tables as defined in the queries in the list create_table_queries in sql_queries

        Args:
                session: SQLAlchemy sessionmaker
    """
    for query in create_table_queries:
        logging.info('Starting query {}'.format(query))
        session.connection().connection.set_isolation_level(0)
        session.execute(query)
        session.connection().connection.set_isolation_level(1)
        logging.info('Query completed')
        
        
def drop_schema(session):
    """Drops schemas as defined in the queries in the list drop_schm_query in sql_queries

        Args:
                session: SQLAlchemy sessionmaker
    """
    for query in drop_schm_query:
        logging.info('Starting query {}'.format(query))
        session.connection().connection.set_isolation_level(0)
        session.execute(query)
        session.connection().connection.set_isolation_level(1)
        logging.info('Query completed')


def main():
    config = configparser.ConfigParser()
    config.read('./credentials/dwh.cfg')
    
    DB_USER = config['DB']['DB_USER']
    DB_PASSWORD = config['DB']['DB_PASSWORD']
    ENDPOINT = config['DB']['HOST']
    DB_PORT = config['DB']['DB_PORT']
    DB_NAME = config['DB']['DB_NAME']

    ARN = config['IAM_ROLE']['ARN']

    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, 
                                                                         ENDPOINT, DB_PORT, DB_NAME))
    session = sessionmaker(bind=engine)()

    create_schema(session, DB_NAME, ARN)
        
    try:    
        create_tables(session)
    except:
        drop_tables(session)
        create_tables(session)


if __name__ == "__main__":
    main()