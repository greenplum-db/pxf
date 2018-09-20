import os, string, sys
import tinctest
from qautils.gppylib.commands.base import Command, REMOTE
from tinctest.lib import local_path, run_shell_command#, Gpdiff

from mpp.lib.PSQL import PSQL
from time import sleep
from storage.lib.checkState import dbState
from mpp.lib.config import GPDBConfig


class Filerepe2e_Util():
    """ 
        Util class for filerep
    """ 

    def wait_till_change_tracking_transition(self):
        """
        PURPOSE:
            Poll till change tracking state achieved: Wait till all segments transition to change tracking state
        @return:
            True [if success] False [if state not in ct for more than 600 secs]
            number of nodes not in ct    
         
        """
        gpcfg = GPDBConfig() 
        num_cl = gpcfg.count_of_nodes_in_mode('c')
        count = 0
        while(int(num_cl)==0):
            tinctest.logger.info("waiting for DB to go into change tracking")
            sleep(30)
            num_cl = gpcfg.count_of_nodes_in_mode('c')
            count = count + 1
            if (count > 80):
               raise Exception("Timed out: cluster not in change tracking")
            tinctest.logger.info("Cluster change tracking")
        return (True,num_cl)

    def inject_fault(self, y = None, f = None, r ='mirror', seg_id = None, H='ALL', m ='async', sleeptime = None, o =None, p=None, outfile=None):
        '''
        PURPOSE : 
            Inject the fault using gpfaultinjector
        @param 
            y : suspend/resume/reset/panic/fault
            f : Name of the faulti
            outfile : output of the command is placed in this file
            rest_of_them : same as in gpfaultinjector help
        '''
        if (not y) or (not f) :
            raise Exception("Need a value for type and name to continue")
        
        if(not os.getenv('MASTER_DATA_DIRECTORY')):
             raise Exception('MASTER_DATA_DIRECTORY environment variable is not set.')
        
    
        fault_cmd = "gpfaultinjector  -f %s -m %s -y %s " % (f, m, y )
        if seg_id :
            fault_cmd = fault_cmd + " -s %s" % seg_id
        if sleeptime :
            fault_cmd = fault_cmd + " -z %s" % sleeptime
        if o:
            fault_cmd = fault_cmd + " -o %s" % o
        if p :
            fault_cmd = fault_cmd + " -p %s" % p
        if seg_id is None :
            fault_cmd = fault_cmd + " -H %s -r %s" % (H, r) 
        if sleeptime :
            fault_cmd = fault_cmd + " --sleep_time_s %s " % sleeptime
        if outfile !=  None:
            fault_cmd = fault_cmd + ">" +outfile 
            
        cmd = Command('fault_command', fault_cmd)
        cmd.run()
        result = cmd.get_results()
        if result.rc != 0 and  y != 'status':
            ok = False
            out = result.stderr
        else:
            ok =  True
            out = result.stdout
        
        if not ok and y != 'status':
            raise Exception("Failed to inject fault %s to %s" % (f,y))
        else:
            tinctest.logger.info('Injected fault %s ' % fault_cmd)
            return (ok,out)
       
    
    def check_fault_status(self,fault_name = None, status = None, max_cycle=10):
        ''' 
        Check whether a fault is triggered. Poll till the fault is triggered
        @param name : Fault name
        @param status : Status to be checked - triggered/completed
        '''
        if (not fault_name) or (not status) :
            self.fail("Need a value for fault_name and status to continue")
    
        poll =0
        while(poll < max_cycle):
            (ok, out) = self.inject_fault(f=fault_name, y='status', r='primary')
            poll +=1
            for line in out.splitlines():
                if line.find(fault_name) > 0 and line.find(status) > 0 : 
                    print "Fault %s is %s " % (fault_name,status)
                    poll = 0 
                    return True
            #sleep a while before start polling again
            sleep(10)   
        return False


