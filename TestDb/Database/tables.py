from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, func

from TestDb.Database import Database

metadata = MetaData()

tags = Table('tags', metadata,
             Column('id', Integer, primary_key=True),
             Column('tag', String))

records = Table('records', metadata,
                Column('id', Integer, primary_key=True),
                Column('path', String),
                Column('time', DateTime(), server_default=func.now()))

record_tags = Table('record_tags', metadata,
                    Column('record_id', ForeignKey('records.id'), primary_key=True),
                    Column('tag_id', ForeignKey('tags.id'), primary_key=True))


def create_tables(database="test_db", host="192.168.1.185", port=54321):
    db = Database(database=database, host=host, port=port)
    # metadata.drop_all(bind=db.engine)
    metadata.create_all(db.engine)


if __name__ == '__main__':
    create_tables()