# -*- coding:utf-8 -*-
import logging, time
from zkpg_cluster_agent import PgClusterAgent
from cluster_conf import pg_cluster_conf

pg_cluster =  pg_cluster_conf()

# main work flow
def main():
    # logging setting
    logger = logging.getLogger()  
    logger.setLevel(logging.INFO)  
    sh = logging.StreamHandler()  
    formatter = logging.Formatter('%(asctime)s -%(module)s:%(filename)s-L%(lineno)d-%(levelname)s: %(message)s')  
    #formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    sh.setFormatter(formatter)  
    logger.addHandler(sh)

    # register local pgdb info
    pg_cluster_agent = PgClusterAgent(logger=logger)
    pg_cluster_agent.register_pg_local_role()

    while True:
        pg_cluster_agent.check_pg_cluster_failover()
        time.sleep(pg_cluster['check_failover_interval'])

# debug main
main()

