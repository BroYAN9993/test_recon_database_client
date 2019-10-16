def stmt_exec(engine, stmt):
    connection = engine.connect()
    trans = connection.begin()
    try:
        result = connection.execute(stmt)
        trans.commit()
        return result
    except:
        trans.rollback()
        raise ValueError