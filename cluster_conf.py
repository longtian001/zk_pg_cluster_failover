def zk_cluster_conf():
    zk_cluster = {
        'hosts':'192.168.31.130:2181,192.168.31.131:2181,192.168.31.132:2181',
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
        'check_recovery_done_interval':3,
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
        'pg_home':'/opt/pgsql/10.3',
        'pg_data':'/pgdata10',
        'asynchronous_standby_user_flag':'user=',
        'recovery_target_key' :'recovery_target_timeline',
        'recovery_target_flag' :"recovery_target_timeline = 'latest'",
        'recovery_target_del_flag' : 0,
        'pg_postgresql_conf':'postgresql.conf',
        'recovery_file':'recovery.conf',
        'recovery_done_file':'recovery.done',
        'sync_standby_flag':'application_name',
        'trigger_file':'/tmp/trigger_failover',
        'async_standby_add_trigger_file':"trigger_file = '/tmp/trigger_failover'",
        'pg_boot_script':'/etc/init.d/postgresql',
        'pg_boot_ok_flag':'ok'
    }
    return pg_local
