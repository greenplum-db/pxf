#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#

from  gppylib import gplog

logger=gplog.get_default_logger()


#################
def validate_yesno( input,default ):
    if input == '':
        return True if default.upper() == 'Y' else False
    elif input.upper().rstrip() in ('Y', 'YES'):
        return True
    elif input.upper().rstrip() in ('N', 'YES'):
        return False
    else:
        return None
        
def ask_yesno(bg_info,msg,default):
    help=" Yy|Nn (default=%s)" % default
    return ask_input(bg_info,msg,help,default,validate_yesno)

#################
def validate_list(input,default):
    if input == '':
        return default
    return input.split(',')
    

def ask_list(bg_info,msg,default):
    help=default
    return ask_input(bg_info,msg,help,default,validate_list)    
 
#################
def validate_int(input,default,min,max):
    if input == '':
        return default    
    numval=int(input)
    
    if numval < min or numval > max:
        return None    
    return numval

def ask_int(bg_info,msg,help,default,min,max):    
    help=" (default=%d)" % default
    return ask_input(bg_info,msg,help,default,validate_int,min,max)

#################     
def validate_string(input,default,listVals):
    if input == '':
        return default    
    
    for val in listVals:
        if input.lower().strip() == val:
            return input
            
    return None

def ask_string(bg_info,msg,default,listVals):
    possvals='|'.join(listVals)    
    help="\n %s (default=%s)" % (possvals,default)
    return ask_input(bg_info,msg,help,default,validate_string,listVals)

#################  
   

def ask_input(bg_info,msg,help,default,validator, *validator_opts):
    if bg_info is not None:             
        print "%s\n" % bg_info
    
    val = None
    while True:
        val = raw_input("%s%s:\n> " % (msg,help))
        
        retval = validator(val,default, *validator_opts)
        
        
        if retval is not None:
            return retval        
        else:
            print "Invalid input: '%s'\n" % val
            
            