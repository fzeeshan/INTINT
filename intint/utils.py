import sys
import psycopg2
import functools
import logging

def safeexecute_pgsql(pool, query, params, connStr, group=None):
    # wrapper for postgresql commit - reconnects on error
    try:
        conn = pool.getconn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params)
        conn.commit()
    except:
        logging.error("Unexpected error (reopening cursor): %s", sys.exc_info()[0])
        try:
            cur.close()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            logging.error("Unexpected error (reopening connection): %s", sys.exc_info()[0])
            pool.putconn(conn)
            conn = pool.getconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(query, params)
            conn.commit()
        except:
            conn.rollback()
    return cur

def sum_dict(a, b):
    r = dict()
    for key in a.keys():
        r[key] = a[key] + b[key]
    return r
