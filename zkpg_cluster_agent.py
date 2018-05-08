# -*- coding:utf-8 -*-
import os, subprocess, time, re, socket
from kazoo.client import KazooClient
from kazoo.client import KazooState
from cluster_conf import zk_cluster_conf, \
     pg_cluster_conf, pg_local_conf

# get item from cluster_conf
zk_cluster = zk_cluster_conf()
pg_cluster = pg_cluster_conf()
pg_local = pg_local_conf()




class PgClusterAgent():
    """PgClsASAgent: PostgreSQL_Cluster_AutoSwitch_Agent
    function:
        1.init pg_db cluster info in zookeeper cluster(cluster_name, primary).
        2.session expire reconnect to zookeeper cluster, regist local pg_db node info again.
        3.watch others pg_db node in zookeeper cluster lost event_type primary_lost or sync_standby_lost
        4.judge local pg_db role sync_standby or async_standby from recovery.conf, according to event_type
        5.promote local pg_db role change sync_standby-->primary or async_standby-->sync_standby,
          according to event_type
    """
    def __init__(self, logger=None):
        # zookeeper cluster
        # init
        self.zk_cluster_hosts = zk_cluster['hosts']
        self.zk_cluster_timeout = zk_cluster['timeout']
        self.zk_cluster_pg_cluster_path = "/" + pg_cluster['name']
        self.zk_cluster_pg_primary_path = self.zk_cluster_pg_cluster_path +'/' + pg_cluster['primary_name']
        self.zk_cluster_pg_sync_standby_path = self.zk_cluster_pg_cluster_path +'/' + pg_cluster['sync_standby_name']
        self.zk_cluster_pg_async_standby_path = self.zk_cluster_pg_cluster_path +'/' + pg_cluster['async_standby_name']
        # create pg local pg role
        self.zk_cluster_runtime_register_pg_role_ephemeral_path = None
        self.zk_cluster_runtime_register_pg_role_ephemeral_node = None
        self.zk_cluster_runtime_register_pg_role_ephemeral_exist_node = None
        # check failover
        self.zk_cluster_runtime_check_pg_role_ephemeral_path = None
        self.zk_cluster_runtime_check_pg_role_ephemeral_node = None
        self.zk_cluster_runtime_check_pg_role_ephemeral_exist_node = None
        self.zk_cluster_runtime_pg_check_upstream_path = None
        # zookeeper client
        self.zk_client = None
        # postgresql cluster
        self.pg_cluster_primary_name = pg_cluster['primary_name']
        self.pg_cluster_sync_standby_name = pg_cluster['sync_standby_name']
        self.pg_cluster_async_standby_name = pg_cluster['async_standby_name']
        # check recovery ip
        self.pg_cluster_check_ip_retries_times = pg_cluster['check_ip_retries_times']
        self.pg_cluster_check_ip_retries_interval = pg_cluster['check_ip_retries_interval']
        self.pg_cluster_check_ip_count = pg_cluster['check_ip_count']
        self.pg_cluster_check_ip_grep_flag = pg_cluster['check_ip_grep_flag']
        self.pg_cluster_check_ip_up_flag = pg_cluster['check_ip_up_flag']
        self.pg_cluster_vip_device_name = pg_cluster['vip_device_name']
        self.pg_cluster_vip_device_up_flag = pg_cluster['vip_device_up_flag']
        self.pg_cluster_runtime_vip_device_alive = False
        self.pg_cluster_runtime_pg_recovery_ip_alive = False
        # postgresql local
        self.pg_local_host = socket.gethostbyname(socket.gethostname())
        self.pg_local_pg_data = pg_local['pg_data']
        self.pg_local_recovery_file = pg_local['pg_data'] + '/' + pg_local['recovery_file']
        self.pg_local_trigger_file = pg_local['trigger_file']
        self.pg_local_sync_standby_flag = pg_local['sync_standby_flag']
        self.pg_local_runtime_pg_role = None
        self.pg_local_runtime_pg_register_role = False
        self.pg_local_runtime_pg_recovery_ip = False
        self.pg_local_runtime_pg_trigger_file_touch = False
        # init steps
        self.logger = logger
        self.init_zk_conn()
    # conn zookeeper cluster, primary or standby
    def init_zk_conn(self):
        # PostgreSQL Cluster Agent connect ZooKeeper Cluster and init pg_cluster_root_path
        try:
            self.zk_client = KazooClient(
                    hosts=self.zk_cluster_hosts, 
                    logger=self.logger, 
                    timeout=self.zk_cluster_timeout
                )
            self.zk_client.start()
            # pg_cluster_root_path: persistent node, "/pg_cluster01"
            # Ensure a path, create if necessary
            self.zk_client.ensure_path(self.zk_cluster_pg_cluster_path)
            self.zk_client.ensure_path(self.zk_cluster_pg_primary_path)
            self.zk_client.ensure_path(self.zk_cluster_pg_sync_standby_path)
            self.zk_client.ensure_path(self.zk_cluster_pg_async_standby_path)
            self.info_str = "init zookeeper cluster postgresql cluster path:\n" \
                + "'%s'.\n '%s'.\n '%s'.\n '%s'.\n" % (
                    self.zk_cluster_pg_cluster_path,
                    self.zk_cluster_pg_primary_path,
                    self.zk_cluster_pg_sync_standby_path,
                    self.zk_cluster_pg_async_standby_path
                )
            self.logger.info(self.info_str)
        except Exception, ex:
            self.init_ret = False
            self.err_str = "start postgresql cluster agent failed! Exception: %s" % (str(ex))
            self.logger.error(self.err_str)
            return
    # create pg_local_role on zookeeper cluster, primary or standby
    def create_pg_local_role(self):
        self.zk_client.create(
            self.zk_cluster_runtime_register_pg_role_ephemeral_node, 
            value="", 
            ephemeral=True, 
            sequence=False, 
            makepath=True
            )
        self.info_str = "register node '%s' successfull." % (self.zk_cluster_runtime_register_pg_role_ephemeral_node)
        self.logger.info(self.info_str)
        self.pg_local_runtime_pg_register_role = True
    # register pg_local_role, primary or standby
    def register_pg_local_role(self):
        if os.path.exists(self.pg_local_pg_data):
            if not os.path.exists(self.pg_local_recovery_file):
                self.pg_local_runtime_pg_role = self.pg_cluster_primary_name
            else:
                standby_chk_cmd_str = "cat %s |grep %s" % (self.pg_local_recovery_file, self.pg_local_sync_standby_flag)
                standby_chk_cmd_reval =os.popen(standby_chk_cmd_str).read()
                if re.findall(r''+ self.pg_local_sync_standby_flag, standby_chk_cmd_reval):
                    self.pg_local_runtime_pg_role = self.pg_cluster_sync_standby_name
                else:
                    self.pg_local_runtime_pg_role = self.pg_cluster_async_standby_name
            # get pg_local_runtime_pg_role successfull
            self.info_str = "local postgresql database role is '%s'." % (self.pg_local_runtime_pg_role)
            self.logger.info(self.info_str)
        else:
            # get pg_local_runtime_pg_role exeption: $PGDATA not exists
            self.err_str = "$PGDATA : '%s' not exists!" % self.pg_local_pg_data
            self.logger.error(self.err_str)
            return
        
        # get local postgresql database role
        self.zk_cluster_runtime_register_pg_role_ephemeral_path = self.zk_cluster_pg_cluster_path + "/" + str(self.pg_local_runtime_pg_role)
        self.zk_cluster_runtime_register_pg_role_ephemeral_node = self.zk_cluster_runtime_register_pg_role_ephemeral_path + "/" + self.pg_local_host
        self.info_str = "register node '%s' start ...." % self.zk_cluster_runtime_register_pg_role_ephemeral_node
        self.logger.info(self.info_str)
        try:
            pg_role_children = self.zk_client.get_children(self.zk_cluster_runtime_register_pg_role_ephemeral_path)
            # create zk_cluster_runtime_register_pg_role_ephemeral_node if zk_cluster_runtime_register_pg_role_ephemeral_path is null 
            if not pg_role_children:
                self.create_pg_local_role()
            # if zk_cluster_runtime_register_pg_role_ephemeral_path is not null, but zk_cluster_runtime_register_pg_role_ephemeral_node's ip address
            # equal self.pg_local_host. first delete ,then create the same
            elif pg_role_children[0] == self.pg_local_host:
                self.zk_client.delete(self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                self.info_str = "purge node '%s' successfull." % (self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                self.logger.info(self.info_str)
                self.create_pg_local_role()
            else:
                self.zk_cluster_runtime_register_pg_role_ephemeral_exist_node = self.zk_client.get_children(
                        self.zk_cluster_runtime_register_pg_role_ephemeral_path )
                self.err_str = "register node '%s' failed!" % (self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                self.logger.error(self.err_str)
                self.err_str = "register node '%s' already exists!" % (
                       self.zk_cluster_runtime_register_pg_role_ephemeral_path + "/" + self.zk_cluster_runtime_register_pg_role_ephemeral_exist_node[0])
                self.logger.error(self.err_str)
                return
        except Exception, ex:
            self.init_ret = False
            self.err_str = "register node '%s' failed! Exception: %s" % (
                    self.zk_cluster_runtime_register_pg_role_ephemeral_node, 
                    str(ex)
                    )
            self.logger.error(self.err_str)
            return

    def check_pg_cluster_failover(self):
        # pg local pg role already registed
        self.info_str = "check pg cluster failover start ..."
        self.logger.info(self.info_str)
        if self.pg_local_runtime_pg_register_role:
            # primary, no need check
            if self.pg_local_runtime_pg_role == self.pg_cluster_primary_name:
                self.info_str = "pg local role is '%s'. no need to check." % (
                    self.pg_local_runtime_pg_role
                    )
                self.logger.info(self.info_str)
            # sync standby, check primary
            elif self.pg_local_runtime_pg_role == self.pg_cluster_sync_standby_name:
                self.zk_cluster_runtime_check_pg_role_ephemeral_path = self.zk_cluster_pg_primary_path
                # check upstream node path, e.g. /pg_cluster_01/primary
                self.check_zk_cluster_exists_node(self.zk_cluster_runtime_check_pg_role_ephemeral_path)
                # reade pg local recovery.conf's IP Address
                self.get_ip_from_recovery_file()
                self.check_pg_local_upstream_ip_available()
                self.ifup_local_vip()
                
            # async standby, check sync_standby
            elif self.pg_local_runtime_pg_role == self.pg_cluster_async_standby_name:
                self.zk_cluster_runtime_check_pg_role_ephemeral_path = self.zk_cluster_pg_sync_standby_path
                # check upstream node path, e.g. /pg_cluster_01/sync_standby
                self.check_zk_cluster_exists_node(self.zk_cluster_runtime_check_pg_role_ephemeral_path)
                # reade pg local recovery.conf's IP Address
                self.get_ip_from_recovery_file()
                self.check_pg_local_upstream_ip_available()
                
    # check if pg local role upstream node's register_pg_role_ephemeral_path has child node
    # sync or async standby
    def check_zk_cluster_exists_node(self,path):
        self.zk_cluster_runtime_check_pg_role_ephemeral_exist_node = self.zk_client.get_children(path)
        if self.zk_cluster_runtime_check_pg_role_ephemeral_exist_node:
            self.info_str = "pg local role is '%s',check path '%s', node '%s'." % (
                        self.pg_local_runtime_pg_role,
                        path,
                        self.zk_cluster_runtime_check_pg_role_ephemeral_exist_node[0]
                    )
            self.logger.info(self.info_str)
            return True
        else:
            self.info_str = "pg local role is '%s',check path '%s', no node found!" % (
                        self.pg_local_runtime_pg_role,
                        path
                    )
            self.logger.info(self.info_str)
            return 
    # get ip address , sync or async standby
    def get_ip_from_recovery_file(self):
        # get ip from $PGDATA/recovery.conf
        with open(self.pg_local_recovery_file) as recovery_handle:
            file_content=recovery_handle.read()
            self.pg_local_runtime_pg_recovery_ip=re.findall(r'\d+\.\d+\.\d+\.\d+',file_content)
            self.info_str = "get ip '%s' from '%s'." % (
                        self.pg_local_runtime_pg_recovery_ip[0],
                        self.pg_local_recovery_file
                    )
            self.logger.info(self.info_str)

    # ping this ip address, sync or async standby
    def check_pg_local_upstream_ip_available(self):
        # setting check schedule
        tries_left = self.pg_cluster_check_ip_retries_times
        sleep_interval = self.pg_cluster_check_ip_retries_interval
        # init status: suppose upstream ip is unknow
        self.pg_cluster_runtime_pg_recovery_ip_alive = False
        # output hint info
        self.info_str = "host %s check ip '%s' available ..." % (
                        self.pg_local_host,
                        self.pg_local_runtime_pg_recovery_ip[0]
                    )
        self.logger.info(self.info_str)
        # try send query to master
        while tries_left and (not self.pg_cluster_runtime_pg_recovery_ip_alive):
            tries_left -= 1
            # ping  upstream node ip in recovery.conf file
            ip_ping_cmd_str = "ping -c %d %s | grep '%s' | wc -l" % (
                        self.pg_cluster_check_ip_count, 
                        self.pg_local_runtime_pg_recovery_ip[0],
                        self.pg_cluster_check_ip_grep_flag
                    )
            ip_ping_cmd_result = subprocess.check_output(ip_ping_cmd_str, shell=True)
            check_ip_ping_result = re.findall(r'' + str(self.pg_cluster_check_ip_up_flag), ip_ping_cmd_result)
            # not return ['0'], pring fail
            if not check_ip_ping_result:
                self.info_str = "host %s try ping ip '%s' ..." % (
                        self.pg_local_host,
                        self.pg_local_runtime_pg_recovery_ip[0]
                )
                self.logger.info(self.info_str)
                time.sleep(sleep_interval)
            else:
                # return ['0'], pring successfull
                self.info_str = "host %s try ping ip '%s' successfull." % (
                        self.pg_local_host,
                        self.pg_local_runtime_pg_recovery_ip[0]
                )
                self.logger.info(self.info_str)
                self.pg_cluster_runtime_pg_recovery_ip_alive = True
                break;

        if not self.pg_cluster_runtime_pg_recovery_ip_alive:
            self.err_str = "host %s ping ip '%s' failed %d times." % (
                        self.pg_local_host,
                        self.pg_local_runtime_pg_recovery_ip[0],
                        self.pg_cluster_check_ip_retries_times
                )
            self.logger.error(self.err_str)

    # touch trigger file for failover on sync_standby
    def touch_trigger_file(self):
        failover_file = self.pg_local_trigger_file
        with open(failover_file, 'a'):
          try:
            # Set current time anyway
            os.utime(failover_file, None)
            self.info_str = "sync_standby host %s will becomes primary and allow write/read operations." % (
                        self.pg_local_host
                )
            self.logger.info(self.info_str)
            self.pg_local_runtime_pg_trigger_file_touch = True
          except OSError:
            pass

    # ifup local vip
    def ifup_local_vip(self):
        vip_device_name = self.pg_cluster_vip_device_name
        ifup_cmd_str = "ifup %s" % vip_device_name
        vip_up_flag = self.pg_cluster_vip_device_up_flag
        chkifup_cmd_str = "nmcli -p d|grep %s|awk '{print $3}'" % vip_device_name
        try:
            # ifup vip network device
            ifup_cmd_result = subprocess.check_output(ifup_cmd_str, shell=True)
            # check result of ifup
            chkifup_cmd_result = subprocess.check_output(chkifup_cmd_str, shell=True)
            chkifup_result = re.findall(r'' + vip_up_flag, chkifup_cmd_result)
            # if the status of vip network device 'connected' is ok
            if chkifup_result:
                self.info_str = "vip network device %s up successfull." % vip_device_name
                self.logger.info(self.info_str)
                self.pg_cluster_runtime_vip_device_alive = True
                done = True
        except Exception:
            pass




