

import sqlalchemy
import json
from time import sleep
from sqlalchemy import exc
import random

default_config_file = "config.json"
engine = None
tunnel = None

def create_db_engine(config = None):

    global engine
    global tunnel

    if engine is not None:
        return engine

    if config is None:
        with open(default_config_file) as f:
            config = json.load(f)["extern_db_config"]

    
    if config["tunnel"]["use_tunnel"]:    
        from sshtunnel import SSHTunnelForwarder

        tunnel_config = config["tunnel"]["tunnel_config"]
        tunnel = SSHTunnelForwarder(
            (tunnel_config["ssh"], tunnel_config["ssh_port"]),
            ssh_username = tunnel_config["user"],
            ssh_password = tunnel_config["password"],
            remote_bind_address = (tunnel_config["remote"], int(tunnel_config["remote_port"])),
            local_bind_address = (tunnel_config["local"], int(tunnel_config["local_port"])) if tunnel_config["local_port"] is not None else (tunnel_config["local"], )
        )

        tunnel.start()

    #create and populate sql configuration from config file
    sql_config = {}
    sql_config["lang"] = config["lang"]
    sql_config["connector"] = config["connector"]
    sql_config["password"] = config["password"]
    sql_config["db_name"] = config["db_name"]
    sql_config["user"] = config["user"]
    sql_config["port"] = config["port"] if tunnel is None else tunnel.local_bind_port
    sql_config["address"] = config["address"] if tunnel is None else tunnel.local_bind_host

    SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (sql_config["lang"], sql_config["connector"], sql_config["user"], sql_config["password"], sql_config["address"], sql_config["port"], sql_config["db_name"])

    #create engine from URI
    engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)

    return engine


def cleanup_db_engine():

    global engine
    global tunnel

    if engine is not None:
        engine.dispose()
        engine = None
    if tunnel is not None:
        tunnel.stop()
        tunnel = None


def engine_exec_r(query, params, retry, delay = 0):
    global engine
    if engine is None:
        raise Exception("create_db_engine must be called before executing on engine")

    if retry < 0:
        raise Exception("Retry limit exceeded")
    #HANDLE THIS IN CALLER
    # #do nothing if empty list (note batch size limiting handled in caller in this case)
    # elif len(batch) > 0:
    sleep(delay)
    res = None
    #engine.begin() block has error handling logic, so try catch should be outside of this block
    #note caller should handle errors and cleanup engine as necessary
    try:
        with engine.begin() as con:
            res = con.execute(query, params) if params is not None else con.execute(query)
    except exc.OperationalError as e:
        #check if deadlock error (code 1213)
        if e.orig.args[0] == 1213:
            backoff = 0
            #if first failure backoff of 0.25-0.5 seconds
            if delay == 0:
                backoff = 0.25 + random.uniform(0, 0.25)
            #otherwise 2-3x current backoff
            else:
                backoff = delay * 2 + random.uniform(0, delay)
            #retry with one less retry remaining and current backoff
            res = engine_exec_r(query, params, retry - 1, backoff)
        #something else went wrong, log exception and add to failures
        else:
            raise e
    #return query result
    return res


def engine_exec(query, params, retry):
    return engine_exec_r(query, params, retry)