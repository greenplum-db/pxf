import os
import time
import tinctest
from qautils.gppylib.commands.base import Command
from tinctest.main import TINCException
from mpp.lib.config import GPDBConfig

class GpRecoversegException(TINCException): pass

class GpRecoverseg():
    '''Class for gprecoverseg utility methods '''

    def __init__(self):
        self.gphome = os.environ.get('GPHOME')

    def run(self,option=' '):
        '''
        @type option: string
        @param option: gprecoverseg option (-F or -r)
        ''' 
        if option not in ('-F' , '-r', ' '):
            raise GpRecoversegException('Not a valid option with gprecoverseg')
        rcvr_cmd = 'gprecoverseg -a  %s' % option
        cmd = Command(name='Run gprecoverseg', cmdStr='source %s/greenplum_path.sh;%s' % (self.gphome, rcvr_cmd))
        tinctest.logger.info("Running gprecoverseg : %s" % cmd)
        cmd.run(validateAfter=True)
        result = cmd.get_results()
        if result.rc != 0 or result.stderr:
            return False
        return True

    def wait_till_insync_transition(self):
        pass


class GpRecover(GpRecoverseg):
    '''Class for gprecoverseg utility methods '''

    MAX_COUNTER=20

    def __init__(self):
        self.gphome = os.environ.get('GPHOME')
        self.config = GPDBConfig()

    def incremental(self):
        '''Incremental Recoverseg '''
        tinctest.logger.info('Running Incremental gprecoverseg...')
        return self.run()

    def full(self):
        '''Full Recoverseg '''
        tinctest.logger.info('Running Full gprecoverseg...')
        return self.run(option = '-F')

    def rebalance(self):
        '''Run gprecoverseg to rebalance the cluster '''
        tinctest.logger.info('Running gprecoverseg rebalance...')
        return self.run(option = '-r')

    def wait_till_insync_transition(self):
        '''
            Poll till all the segments transition to insync state. 
            Number of trials set to MAX_COUNTER
        '''
        counter= 1
        while(not self.config.is_not_insync_segments()):
            if counter > self.MAX_COUNTER:
                raise Exception('Segments did not come insync after 20 minutes')
            else:
                counter = counter + 1
                time.sleep(60) #Wait 1 minute before polling again
        tinctest.logger.info('Segments are synchronized ...')
        return True
        
