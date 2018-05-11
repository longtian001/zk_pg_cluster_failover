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
        self.zk_cluster_runtime_check_pg_upstream_ephemeral_node = None
        self.zk_cluster_runtime_check_pg_master_ephemeral_node = None
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
        self.pg_cluster_check_recovery_done_interval = pg_cluster['check_recovery_done_interval']
        self.pg_cluster_runtime_vip_device_alive = False
        self.pg_cluster_runtime_pg_recovery_ip_alive = False
        # postgresql local
        self.pg_local_host = socket.gethostbyname(socket.gethostname())
        self.pg_local_pg_home = pg_local['pg_home']
        self.pg_local_pg_data = pg_local['pg_data']
        self.pg_local_pg_postgresql_conf = self.pg_local_pg_data + '/' + pg_local['pg_postgresql_conf']
        self.pg_local_asynchronous_standby_user_flag = pg_local['asynchronous_standby_user_flag']
        self.pg_local_recovery_target_key = pg_local['recovery_target_key']
        self.pg_local_recovery_target_flag = pg_local['recovery_target_flag']
        self.pg_local_recovery_target_del_flag = pg_local['recovery_target_del_flag']
        self.pg_local_recovery_file = self.pg_local_pg_data + '/' + pg_local['recovery_file']
        self.pg_local_recovery_done_file = self.pg_local_pg_data + '/' + pg_local['recovery_done_file']
        self.pg_local_trigger_file = pg_local['trigger_file']
        self.pg_local_async_standby_add_trigger_file = pg_local['async_standby_add_trigger_file']
        self.pg_local_sync_standby_flag = pg_local['sync_standby_flag']
        self.pg_local_sync_standby_full_flag = self.pg_local_sync_standby_flag + "=s1 "
        self.pg_local_pg_boot_script = pg_local['pg_boot_script']
        self.pg_local_pg_boot_ok_flag = pg_local['pg_boot_ok_flag']
        self.pg_local_runtime_pg_role = None
        self.pg_local_runtime_pg_register_role = False
        self.pg_local_runtime_pg_recovery_ip = False
        self.pg_local_runtime_new_sync_recovery_ip = False
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
            self.err_str = "start postgresql cluster agent failed! Exception: '%s'" % (str(ex))
            self.logger.error(self.err_str)
            return
    # create pg_local_role on zookeeper cluster, primary or standby
    def create_pg_local_role(self, node):
        done = False
        try:
            self.zk_client.create(
                node,
                value="", 
                ephemeral=True, 
                sequence=False, 
                makepath=True
                )
            self.info_str = "register node '%s' successfull." % (node)
            self.logger.info(self.info_str)
            self.pg_local_runtime_pg_register_role = True
            done = True
        except Exception, ex:
            self.init_ret = False
            self.err_str = "create pg local role failed! Exception: '%s'" % (str(ex))
            self.logger.error(self.err_str)
            return
        return done
    # register pg_local_role, primary or standby
    def register_pg_local_role(self):
        done = False
        if os.path.exists(self.pg_local_pg_data):
            if not os.path.exists(self.pg_local_recovery_file):
                self.pg_local_runtime_pg_role = self.pg_cluster_primary_name
            else:
                standby_chk_cmd_str = "cat '%s' |grep '%s'" % (self.pg_local_recovery_file, self.pg_local_sync_standby_flag)
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
                self.create_pg_local_role(self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                done = True
            # if zk_cluster_runtime_register_pg_role_ephemeral_path is not null, but zk_cluster_runtime_register_pg_role_ephemeral_node's ip address
            # equal self.pg_local_host. first delete ,then create the same
            elif pg_role_children[0] == self.pg_local_host:
                self.zk_client.delete(self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                self.info_str = "purge node '%s' successfull." % (self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                self.logger.info(self.info_str)
                self.create_pg_local_role(self.zk_cluster_runtime_register_pg_role_ephemeral_node)
                done = True
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
            self.err_str = "register node '%s' failed! Exception: '%s'" % (
                    self.zk_cluster_runtime_register_pg_role_ephemeral_node, 
                    str(ex)
                    )
            self.logger.error(self.err_str)
            return
        return done

    #get child node from given path
    def get_zk_cluster_node_ip(self,node_path):
        done = False
        try: 
            pg_role_ephemeral_node_list = self.zk_client.get_children(node_path)
            if pg_role_ephemeral_node_list:
                self.pg_local_runtime_new_sync_recovery_ip = pg_role_ephemeral_node_list[0]
                self.info_str = "pg local role is '%s',check node path '%s' node %s ok." % (
                            self.pg_local_runtime_pg_role,
                            node_path,
                            self.pg_local_runtime_new_sync_recovery_ip
                        )
                self.logger.info(self.info_str)
                done = True
            else:
                self.err_str = "pg local role is '%s',check node path '%s' failed!" % (
                            self.pg_local_runtime_pg_role,
                            node_path
                        )
                self.logger.error(self.err_str)
        except Exception, ex:
            self.init_ret = False
            self.err_str = "connect zookeeper cluster failed! Exception: '%s'" % (str(ex))
            self.logger.error(self.err_str)
        
        return done

    # check if pg local role upstream node's register_pg_role_ephemeral_path has child node
    # sync or async standby
    def check_zk_cluster_exists_node(self,path):
        done = False
        try: 
            pg_role_ephemeral_exist_node = self.zk_client.exists(path)
            if pg_role_ephemeral_exist_node:
                self.info_str = "pg local role is '%s',check node '%s' ok." % (
                            self.pg_local_runtime_pg_role,
                            path
                        )
                self.logger.info(self.info_str)
                done = True
            else:
                self.err_str = "pg local role is '%s',check node '%s' failed!" % (
                            self.pg_local_runtime_pg_role,
                            path
                        )
                self.logger.error(self.err_str)
        except Exception, ex:
            self.init_ret = False
            self.err_str = "connect zookeeper cluster failed! Exception: '%s'" % (str(ex))
            self.logger.error(self.err_str)
        
        return done

    # get ip address , sync or async standby
    def get_ip_from_recovery_file(self):
        done = False
        try:
            # get ip from $PGDATA/recovery.conf
            with open(self.pg_local_recovery_file) as recovery_handle:
                file_content=recovery_handle.read()
                pg_local_runtime_pg_recovery_ip_list =re.findall(r'\d+\.\d+\.\d+\.\d+',file_content)
                self.pg_local_runtime_pg_recovery_ip = pg_local_runtime_pg_recovery_ip_list[0]
                self.info_str = "get ip '%s' from '%s'." % (
                            self.pg_local_runtime_pg_recovery_ip,
                            self.pg_local_recovery_file
                        )
                self.logger.info(self.info_str)
                done = True
        except Exception, ex:
            self.init_ret = False
            self.err_str = "get ip from '%s' failed! Exception: '%s'" % (
                    self.pg_local_recovery_file, 
                    str(ex)
                    )
            self.logger.error(self.err_str)
            return
        return done

    # ping this ip address, sync or async standby
    def check_ip_available(self, ip):
        # setting check schedule
        tries_left = self.pg_cluster_check_ip_retries_times
        sleep_interval = self.pg_cluster_check_ip_retries_interval
        # init: check status
        done = False
        # output hint info
        self.info_str = "host '%s' check ip '%s' available ..." % (
                        self.pg_local_host,
                        ip
                    )
        self.logger.info(self.info_str)
        # try send query to master
        while tries_left and (not done):
            tries_left -= 1
            # ping  upstream node ip in recovery.conf file
            ip_ping_cmd_str = "ping -c %d '%s' | grep '%s' | wc -l" % (
                        self.pg_cluster_check_ip_count, 
                        ip,
                        self.pg_cluster_check_ip_grep_flag
                    )
            ip_ping_cmd_result = subprocess.check_output(ip_ping_cmd_str, shell=True)
            check_ip_ping_result = re.findall(r'' + str(self.pg_cluster_check_ip_up_flag), ip_ping_cmd_result)
            # not return ['0'], pring fail
            if not check_ip_ping_result:
                self.info_str = "host '%s' try ping ip '%s' ..." % (
                        self.pg_local_host,
                        ip
                )
                self.logger.info(self.info_str)
                time.sleep(sleep_interval)
            else:
                # return ['0'], pring successfull
                self.info_str = "host '%s' try ping ip '%s' successfull." % (
                        self.pg_local_host,
                        ip
                )
                self.logger.info(self.info_str)
                done = True
                break;

        if not done:
            self.err_str = "host '%s' ping ip '%s' failed %d times." % (
                        self.pg_local_host,
                        ip,
                        self.pg_cluster_check_ip_retries_times
                )
            self.logger.error(self.err_str)

        return done

    # touch trigger file for failover on sync_standby
    def touch_trigger_file(self):
        failover_file = self.pg_local_trigger_file
        done = False
        with open(failover_file, 'a'):
          try:
            # Set current time anyway
            os.utime(failover_file, None)
            self.info_str = "sync_standby host '%s' will becomes primary and allow write/read operations." % (
                        self.pg_local_host
                )
            self.logger.info(self.info_str)
            done = True
          except OSError:
            pass
        return done

    # find $PGDATA/recovery.done file
    def find_recovery_done_file_exists(self,file):
        done = False
        while True:
            recovery_done_file_exists = os.path.exists(file)
            if recovery_done_file_exists:
                done = True
                break
            time.sleep(self.pg_cluster_check_recovery_done_interval)
        return done

    # ifup local vip
    def ifup_vip_localhost(self, ip):
        vip_device_name = ip
        ifup_cmd_str = "ifup '%s'" % vip_device_name
        vip_up_flag = self.pg_cluster_vip_device_up_flag
        chkifup_cmd_str = "nmcli -p d|grep '%s'|awk '{print $3}'" % vip_device_name
        done = False
        try:
            # ifup vip network device
            ifup_cmd_result = subprocess.check_output(ifup_cmd_str, shell=True)
            # check result of ifup
            chkifup_cmd_result = subprocess.check_output(chkifup_cmd_str, shell=True)
            chkifup_result = re.findall(r'' + vip_up_flag, chkifup_cmd_result)
            # if the status of vip network device 'connected' is ok
            if chkifup_result:
                self.info_str = "vip network device '%s' up successfull." % vip_device_name
                self.logger.info(self.info_str)
                done = True
        except Exception:
            pass
        return done

    # sync_standby promote, become primary
    def sync_standby_promote(self):
        # reade pg local recovery.conf's IP Address
        ip_exist_recovery_file = self.get_ip_from_recovery_file()
        # if found ip
        if ip_exist_recovery_file:
            # create pg_upstream_ephemeral_node check path
            self.zk_cluster_runtime_check_pg_upstream_ephemeral_node = self.zk_cluster_pg_primary_path + '/' \
                            + self.pg_local_runtime_pg_recovery_ip
            # check upstream node path, e.g. /pg_cluster_01/primary/sync_standby_recovery_ip
            zk_cluster_exists_node = self.check_zk_cluster_exists_node(self.zk_cluster_runtime_check_pg_upstream_ephemeral_node)
            # exists pg_upstream_ephemeral_node, return check_pg_cluster_failover
            if zk_cluster_exists_node:
                pass
            # if not , check pg_local_upstream_ip_available(host in recovery.conf)
            else:
                pg_local_upstream_ip_available = self.check_ip_available(self.pg_local_runtime_pg_recovery_ip)
                # if pg_local_upstream_ip_available is true, return check_pg_cluster_failover
                if pg_local_upstream_ip_available:
                    self.info_str = "host '%s' pg role '%s' check ip '%s' ok." % (
                            self.pg_local_host, 
                            self.pg_local_runtime_pg_role,
                            self.pg_local_runtime_pg_recovery_ip
                        )
                    self.logger.info(self.info_str)
                else:
                    # if pg_local_upstream_ip_available is false, generate trigger file for promote
                    self.err_str = "host '%s' pg role '%s' check upstream ip '%s' failed!" % (
                            self.pg_local_host, 
                            self.pg_local_runtime_pg_role,
                            self.pg_local_runtime_pg_recovery_ip
                        )
                    self.logger.error(self.err_str)
                    touch_trigger_file_result = self.touch_trigger_file()
                    if touch_trigger_file_result:
                        self.info_str = "host '%s' pg role '%s' generate trigger file '%s' ok." % (
                            self.pg_local_host, 
                            self.pg_local_runtime_pg_role,
                            self.pg_local_trigger_file
                        )
                        self.logger.info(self.info_str)
                        recovery_done_file_exists = self.find_recovery_done_file_exists(
                                    self.pg_local_recovery_done_file
                                )
                        if recovery_done_file_exists:
                            self.info_str = "host '%s' pg role '%s' generate file '%s'ok." % (
                                    self.pg_local_host,
                                    self.pg_local_runtime_pg_role,
                                    self.pg_local_recovery_done_file
                                )
                            self.logger.info(self.info_str)
                            register_new_master_node = self.register_pg_local_role()
                            if register_new_master_node:
                                self.info_str = "host '%s' pg role '%s' registe node '%s' ok." % (
                                        self.pg_local_host,
                                        self.pg_local_runtime_pg_role,
                                        self.zk_cluster_runtime_register_pg_role_ephemeral_node
                                    )
                                self.logger.info(self.info_str)
                                ifup_vip_ok = self.ifup_vip_localhost(self.pg_cluster_vip_device_name)
                                if ifup_vip_ok:
                                    self.info_str = "host '%s' pg role '%s' ifup '%s' ok." % (
                                        self.pg_local_host, 
                                        self.pg_local_runtime_pg_role,
                                        self.pg_cluster_vip_device_name
                                    )
                                    self.logger.info(self.info_str)
                                    del_trigger_file = self.delete_file(self.pg_local_trigger_file)
                                    if del_trigger_file:
                                        self.info_str = "host '%s' pg role '%s' delete trigger file '%s' ok." % (
                                            self.pg_local_host, 
                                            self.pg_local_runtime_pg_role,
                                            self.pg_local_trigger_file
                                        )
                                        self.logger.info(self.info_str)
                                        del_recovery_done_file = self.delete_file(self.pg_local_recovery_done_file)
                                        if del_recovery_done_file:
                                            self.info_str = "host '%s' pg role '%s' delete recovery done file '%s' ok." % (
                                                self.pg_local_host, 
                                                self.pg_local_runtime_pg_role,
                                                self.pg_local_recovery_done_file
                                            )
                                            self.logger.info(self.info_str)
                                            pg_local_role_old_node = self.zk_cluster_pg_sync_standby_path + '/' + self.pg_local_host
                                            del_pg_local_role_old_node = self.delete_pg_local_role_old_node(pg_local_role_old_node)
                                            if del_pg_local_role_old_node:
                                                self.info_str = "host '%s' pg role '%s' delete node '%s' ok." % (
                                                    self.pg_local_host, 
                                                    self.pg_local_runtime_pg_role,
                                                    pg_local_role_old_node
                                                )
                                                self.logger.info(self.info_str)

    # delete trigger file
    def delete_file(self, file):
        done = False
        while True:
            os.remove(file)
            if not os.path.exists(file):
                done = True
                break
        return True

    # delete old pg sync_standby node in zk cluster
    def delete_pg_local_role_old_node(self,node):
        done = False
        try:
            self.zk_client.delete(
                    node, 
                    recursive=True
                )
            done = True
        except Exception, ex:
            self.init_ret = False
            self.err_str = "delete pg local role failed! Exception: '%s'" % (str(ex))
            self.logger.error(self.err_str)
            return
        return done

    # check sync_standby node fail, then async_standby transform to sync_standby
    def async_standby_transform(self):
        # reade pg local recovery.conf's IP Address
        ip_exist_recovery_file = self.get_ip_from_recovery_file()
        # if found ip
        if ip_exist_recovery_file:
            # create pg_upstream_ephemeral_node check path
            self.zk_cluster_runtime_check_pg_upstream_ephemeral_node = self.zk_cluster_pg_sync_standby_path + '/' \
                            + self.pg_local_runtime_pg_recovery_ip
            # check upstream node path, e.g. /pg_cluster_01/sync_standby/sync_ip
            zk_cluster_exists_node = self.check_zk_cluster_exists_node(self.zk_cluster_runtime_check_pg_upstream_ephemeral_node)
            # exists pg_upstream_ephemeral_node, return check_pg_cluster_failover
            if zk_cluster_exists_node:
                pass
            else:
                # check /pg_cluster_01/primary/sync_ip node exists
                self.zk_cluster_runtime_check_pg_master_ephemeral_node = self.zk_cluster_pg_primary_path + "/" \
                            + self.pg_local_runtime_pg_recovery_ip
                zk_cluster_exists_new_master_node = self.check_zk_cluster_exists_node(
                        self.zk_cluster_runtime_check_pg_master_ephemeral_node
                    )
                if zk_cluster_exists_new_master_node:
                    # check pg_local_upstream_ip_available
                    pg_local_upstream_ip_available = self.check_ip_available(self.pg_local_runtime_pg_recovery_ip)
                    # if pg_local_upstream_ip_available is true, return check_pg_cluster_failover
                    if pg_local_upstream_ip_available:
                        self.info_str = "host '%s' pg role '%s' check ip '%s' ok." % (
                                self.pg_local_host, 
                                self.pg_local_runtime_pg_role,
                                self.pg_local_runtime_pg_recovery_ip
                            )
                        self.logger.info(self.info_str)
                        # sync_standby fail,, continue async_standby_switch_rest
                        self.async_standby_switch_rest()
                else:
                    # get primary_ip from /pg_cluster_01/primary/ path
                    get_primary_ip = self.get_zk_cluster_node_ip(self.zk_cluster_pg_primary_path)
                    if get_primary_ip :
                        self.zk_cluster_runtime_check_pg_master_ephemeral_node = self.zk_cluster_pg_primary_path + "/" \
                            + self.pg_local_runtime_new_sync_recovery_ip
                        # if /pg_cluster_01/primary/primary_ip exists
                        zk_cluster_exists_new_master_node = self.check_zk_cluster_exists_node(
                                self.zk_cluster_runtime_check_pg_master_ephemeral_node
                            )
                        if zk_cluster_exists_new_master_node:
                            self.info_str = "host '%s' pg role '%s' check ip '%s' ok." % (
                                self.pg_local_host, 
                                self.pg_local_runtime_pg_role,
                                self.pg_local_runtime_new_sync_recovery_ip
                            )
                            self.logger.info(self.info_str)
                            # update recovery.conf host with primary_ip
                            upt_recovery_host = self.update_recovery_host(
                                    self.pg_local_runtime_pg_recovery_ip, 
                                    self.pg_local_runtime_new_sync_recovery_ip
                                )
                            if upt_recovery_host:
                                self.info_str = "host '%s' pg role '%s' update recovery host from '%s' to '%s' successfull." % (
                                self.pg_local_host, 
                                self.pg_local_runtime_pg_role,
                                self.pg_local_runtime_pg_recovery_ip, 
                                self.pg_local_runtime_new_sync_recovery_ip
                            )
                            self.logger.info(self.info_str)
                            # done async_standby_switch_rest
                            self.async_standby_switch_rest()



    # continue async_standby switch rest
    def async_standby_switch_rest(self):
        done = False
        # delete recovery_target_timeline
        del_recovery_target_timeline = self.delete_recovery_target_timeline()
        if del_recovery_target_timeline:
            self.info_str = "host '%s' pg role '%s' file %s remove '%s' ok." % (
                self.pg_local_host, 
                self.pg_local_runtime_pg_role,
                self.pg_local_recovery_file,
                self.pg_local_recovery_target_flag
            )
            self.logger.info(self.info_str)
            add_recovery_sync_application_name = self.add_recovery_application_name()
            if add_recovery_sync_application_name:
                self.info_str = "host '%s' pg role '%s' file %s add '%s' ok." % (
                    self.pg_local_host, 
                    self.pg_local_runtime_pg_role,
                    self.pg_local_recovery_file,
                    self.pg_local_sync_standby_full_flag
                )
                self.logger.info(self.info_str)
                # add trigger_file in recovery.conf
                async_standby_add_trigger_file = self.add_recovery_trigger_file()
                if async_standby_add_trigger_file:
                    self.info_str = "host '%s' pg role '%s' trigger file '%s' add %s ok." % (
                            self.pg_local_host, 
                            self.pg_local_runtime_pg_role,
                            self.pg_local_recovery_file,
                            self.pg_local_async_standby_add_trigger_file
                        )
                    self.logger.info(self.info_str)
                    # register new sync_standby node in zookeeper cluster
                    register_new_sync_node = self.register_pg_local_role()
                    if register_new_sync_node:
                        self.info_str = "host '%s' pg role '%s' register new sync standby '%s' ok." % (
                            self.pg_local_host, 
                            self.pg_local_runtime_pg_role,
                            self.zk_cluster_runtime_register_pg_role_ephemeral_node
                        )
                        self.logger.info(self.info_str)
                        # delete old async_standby node in zookeeper cluster
                        pg_local_role_old_node = self.zk_cluster_pg_async_standby_path + '/' + self.pg_local_host
                        del_pg_local_role_old_node = self.delete_pg_local_role_old_node(pg_local_role_old_node)
                        if del_pg_local_role_old_node:
                            self.info_str = "host '%s' pg role '%s' delete node '%s' ok." % (
                                    self.pg_local_host, 
                                    self.pg_local_runtime_pg_role,
                                    pg_local_role_old_node
                                )
                            self.logger.info(self.info_str)
                            restart_pgdb_server = self.restart_pg_db_server()
                            if restart_pgdb_server:
                                self.info_str = "host '%s' pg role '%s' restart postgresql database ok." % (
                                        self.pg_local_host, 
                                        self.pg_local_runtime_pg_role
                                    )
                                self.logger.info(self.info_str)
                                done = True

        return done
    
    # delete recovery_target_timeline item in recovery.conf
    def delete_recovery_target_timeline(self):
        done = False
        remove_pg_recovery_cmd_str = "sed -i \"/%s/d\" %s" % (
                self.pg_local_recovery_target_flag,
                self.pg_local_recovery_file
            )
        remove_pg_recovery_cmd_result = subprocess.check_output(remove_pg_recovery_cmd_str, shell=True)

        grep_pg_recovery_cmd_str = "grep %s %s|wc -l" %(
                self.pg_local_recovery_target_key,
                self.pg_local_recovery_file
            )
        grep_pg_recovery_cmd_result = subprocess.check_output(grep_pg_recovery_cmd_str, shell=True)
        chkgrep_pgconf_result = re.findall(
                r'' + str(self.pg_local_recovery_target_del_flag), 
                grep_pg_recovery_cmd_result
            )
        if chkgrep_pgconf_result:
            done = True
        return done

    # update recovery host
    def update_recovery_host(self, old_sync_host, new_sync_host):
        done = False
        # replace old host recovery.conf
        replace_recovery_host_cmd_str = "sed -i s/%s/%s/g %s" % (
                old_sync_host,
                new_sync_host,
                self.pg_local_recovery_file
            )
        replace_recovery_host_cmd_result = subprocess.check_output(replace_recovery_host_cmd_str, shell=True)
        # grep new host in recovery.conf
        grep_recovery_host_cmd_str = "grep %s %s" %(
                new_sync_host,
                self.pg_local_recovery_file
            )
        grep_recovery_host_cmd_result = subprocess.check_output(grep_recovery_host_cmd_str, shell=True)
        chkgrep_recovery_host_result = re.findall(
                r'' + new_sync_host, 
                grep_recovery_host_cmd_result
            )
        if chkgrep_recovery_host_result:
            done = True
        return done

    # add application_name=s1 in recovery.conf
    def add_recovery_application_name(self):
        done = False
        replace_pgrecovery_target_str = self.pg_local_sync_standby_full_flag + self.pg_local_asynchronous_standby_user_flag
        replace_pg_recovery_cmd_str = "sed -i s/\"%s\"/\"%s\"/g %s" % (
                self.pg_local_asynchronous_standby_user_flag,
                replace_pgrecovery_target_str,
                self.pg_local_recovery_file
            )
        replace_pg_recovery_cmd_result = subprocess.check_output(replace_pg_recovery_cmd_str, shell=True)

        grep_pg_recovery_cmd_str = "grep %s %s" %(
                self.pg_local_sync_standby_flag,
                self.pg_local_recovery_file
            )
        grep_pg_recovery_cmd_result = subprocess.check_output(grep_pg_recovery_cmd_str, shell=True)
        chkgrep_pg_recovery_result = re.findall(
                r'' + self.pg_local_sync_standby_flag, 
                grep_pg_recovery_cmd_result
            )
        if chkgrep_pg_recovery_result:
            done = True
        return done
    # async_standby trans to sync_standby add trigger_file in recovery.conf
    def add_recovery_trigger_file(self):
        done = False
        try: 
            # append trigger_file item
            with open(self.pg_local_recovery_file, "a") as myfile:
                myfile.write(self.pg_local_async_standby_add_trigger_file)

            # check trigger_file item in recovery.conf
            grep_add_pgrecovery_trigger_file_cmd_str = "grep %s %s" %(
                    self.pg_local_trigger_file,
                    self.pg_local_recovery_file
                )
            grep_add_pgrecovery_trigger_file_cmd_result = subprocess.check_output(grep_add_pgrecovery_trigger_file_cmd_str, shell=True)
            chkgrep_add_pgrecovery_trigger_file_result = re.findall(
                    r'' + self.pg_local_async_standby_add_trigger_file, 
                    grep_add_pgrecovery_trigger_file_cmd_result
                )
            # check ok return true
            if chkgrep_add_pgrecovery_trigger_file_result:
                done = True
        except Exception, ex:
            self.init_ret = False
            self.err_str = "add %s file %s failed! Exception: '%s'" % (
                    self.pg_local_recovery_file,
                    self.pg_local_async_standby_add_trigger_file,
                    str(ex)
                )
            self.logger.error(self.err_str)

        return done

    # restart local postgresql database
    def restart_pg_db_server(self):
        done = False
        pg_db_restart_cmd_str = self.pg_local_pg_boot_script + "  restart"
        pg_db_restart_cmd_result = subprocess.check_output(pg_db_restart_cmd_str, shell=True)
        chkgrep_pg_db_restart_result = re.findall(
                r'' + self.pg_local_pg_boot_ok_flag, 
                pg_db_restart_cmd_result
            )
        if chkgrep_pg_db_restart_result:
            done = True
        return done

    # check pg cluster failover
    def check_pg_cluster_failover(self):
            # pg local pg role already registed
            self.info_str = "check pg cluster failover start ..."
            self.logger.info(self.info_str)
            if self.pg_local_runtime_pg_register_role:
                # primary, no need check
                if self.pg_local_runtime_pg_role == self.pg_cluster_primary_name:
                    self.info_str = "pg local role is '%s'. no need to failover." % (
                        self.pg_local_runtime_pg_role
                        )
                    self.logger.info(self.info_str)
                # sync standby, check primary
                elif self.pg_local_runtime_pg_role == self.pg_cluster_sync_standby_name:
                    self.sync_standby_promote()

                # async standby, check sync_standby
                elif self.pg_local_runtime_pg_role == self.pg_cluster_async_standby_name:
                    self.async_standby_transform()
                    

