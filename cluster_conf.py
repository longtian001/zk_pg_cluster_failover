def zk_cluster_conf():
    zk_cluster = {
        'hosts':'192.168.31.210:2181,192.168.31.211:2181,192.168.31.212:2181',
        'timeout':10
    }
    return zk_cluster

# check_ip's ip from recovery.conf
def pg_cluster_conf():
    pg_cluster = {
        'name':'pg_cluster_01',
        'primary_name':'primary',
        'sync_standby_name':'sync_standby',
        'async_standby_name':'async_standby',
        'check_failover_interval':10,
        'vip_device_name':'ens37',
        'vip_device_up_flag':'connected',
        'check_ip_retries_times':3,
        'check_ip_retries_interval':1,
        'check_ip_count':3,
        'check_ip_grep_flag':'0 received',
        'check_ip_up_flag': 0
    }
    return pg_cluster

def pg_local_conf():
    pg_local = {
        'pg_home':'/usr/pgsql-10',
        'pg_data':'/var/lib/pgsql/10/data',
        'recovery_file':'recovery.conf',
        'sync_standby_flag':'application_name',
        'trigger_file':'/tmp/trigger_failover'
    }
    return pg_local
