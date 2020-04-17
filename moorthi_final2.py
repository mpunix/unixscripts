#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json
import numpy as np
import getopt
import sys


# In[2]:


def jsonproc(jsonstr):
    disklst=[]
    for json1list in json.loads(jsonstr)["data"]:
        disklst.append({"diskname" : json1list[4], "diskgb" : json1list[5]})
    return {"disklist" :  disklst}


# In[3]:


def read_server_defn(fname,sname):
    df = pd.read_excel (fname, sheet_name='vm',skiprows=1,names=server_sheet_columns)
    fillna_columns=['x86 &VM Definition', 'Hardware Type', 'PCA', 'VM Name']
    for coln in fillna_columns:
        df[coln].fillna(method='ffill',inplace=True)
    return df


# In[4]:


def disks_to_json(vmfiledef):
    vm_disks=vmfiledef[['x86 &VM Definition', 'Hardware Type', 'PCA', 
                        'VM Name','Data Disk','Data Disk Size (GB)']].groupby(
                            ['x86 &VM Definition', 'Hardware Type', 'PCA', 'VM Name']
                        ).apply(
                                lambda x: x.to_json(orient='split')
                               ).to_frame().reset_index()
    
    vm_disks.rename(columns={0:'json1'},inplace=True)  
    vm_disks['diskslist']=vm_disks.json1.apply(lambda x: jsonproc(x))
    vm_defn=vm_disks.join(vmfiledef,
              how='inner',rsuffix='_disk')[[
            'x86 &VM Definition',           'Hardware Type',
                           'PCA',                 'VM Name',
                     'diskslist',                     'vcpu',
                   'Memory (GB)',                 'OS Disk',
                    'Repository',                       'swap',
                       'Virtual',                      'HA',
                          'eth0',                    'eth1',
                          'eth2',                    'eth3',
                          'eth4']
        ]
    return vm_defn


# In[5]:


def read_server_network_defn(fname,sname):
        df = pd.read_excel (fname, sheet_name=sname,skiprows=9,names=network_sheet_columns)
        
            


        
        fillna_columns=['Server']

        indexNames = df[ df['Server'].isna() ].index
        df.drop(indexNames , inplace=True)

        df.replace('Hostname',np.nan,inplace=True)
        
        for coln in fillna_columns:
            df[coln].fillna(method='ffill',inplace=True)
        df_1=df[['Server', 'Host', 
                   'MGMT Site 1', 'MGMT site1', 'MGMT Site 2', 'MGMT Site 2.1',
                   'BCK Site 1', 'Bck Site 2', 'App Site 1', 'App site 2', 'DB site 1',
                   'DB site 2', 'HSecurity1 Site 1', 'HSecurity2 Site 1',
                   'HSecurity1 Site 2', 'NAS Site 1(IP)', 'NAS site2 (IP)', 'App site 1',
                   'App site 2.1']]
        return df_1


# In[6]:


def read_common_network_defn(fname,sname):
    df = pd.read_excel (fname, 
                     sheet_name=sname,
                     nrows=8,
                     header=None,
#                      names=network_defn_fromfile.columns,
                     names=network_sheet_columns,
                     dtype={'Server':'object'}).reset_index()
    
    df=df.drop([0,1,3,5,7]).reset_index()
    df= df[['Server', 'Host', 
       'MGMT Site 1', 'MGMT site1', 'MGMT Site 2', 'MGMT Site 2.1',
       'BCK Site 1', 'Bck Site 2', 'App Site 1', 'App site 2', 'DB site 1',
       'DB site 2', 'HSecurity1 Site 1', 'HSecurity2 Site 1',
       'HSecurity1 Site 2', 'NAS Site 1(IP)', 'NAS site2 (IP)', 'App site 1',
       'App site 2.1']]
    
    return df


# In[7]:


def network_to_json(server_network_df,common_network_df):

    def cartesian_product_basic(left, right):
        return (
       left.assign(key=1).merge(right.assign(key=1), on='key').drop('key', 1))

    def eth_gen1(network_row):
        eth_ctr=0
        nwrklst=[]

        for col in eth_columns:
            #print(col)
            splitstr=network_row[col].strip().split(' ')
            print(len(splitstr))
            if len(splitstr) == 5: 
                nwrklst.append({  "name" : eth_list[eth_ctr] ,
                                  "hostname" :   splitstr[1] ,
                                  "ip" :   splitstr[0] ,
                                  "netmask" :    splitstr[2] ,
                                  "gateway" : splitstr[4] ,
                                  "vlan" :  splitstr[3].split('/')[1] 
                               })
                  
            eth_ctr= eth_ctr+1
    
            return {"network_info" : nwrklst}


    def eth_gen(network_row):
        
        eth_columns=[ 
               'MGMT Site 1', 'MGMT site1', 'MGMT Site 2', 'MGMT Site 2.1',
               'BCK Site 1', 'Bck Site 2', 'App Site 1', 'App site 2', 'DB site 1',
               'DB site 2', 'HSecurity1 Site 1', 'HSecurity2 Site 1',
               'HSecurity1 Site 2', 'NAS Site 1(IP)', 'NAS site2 (IP)', 'App site 1',
               'App site 2.1']
        
        eth_list=['eth0','eth1','eth2','eth3','eth4']
        
        eth_ctr=0
        nwrklst=[]

        for col in eth_columns:
            splitstr=network_row[col].strip().split(' ')
            if len(splitstr) == 5: 
                nwrklst.append({  "name" : eth_list[eth_ctr] ,
                      "hostname" :   splitstr[1] ,
                      "ip" :   splitstr[0] ,
                      "netmask" :    splitstr[2] ,
                      "gateway" : splitstr[4] ,
                      "vlan" :  splitstr[3].split('/')[1] 
                           })
                  
                eth_ctr= eth_ctr+1

        return {"network_info" : nwrklst}


    server_network_df.fillna(value='',inplace=True)
    
    server_network_df_agg=server_network_df.groupby('Server').agg(' '.join).reset_index()
    

    
    common_network_df=common_network_df.astype('str')

    uniqueservers_df=pd.DataFrame(server_network_df_agg.Server.unique(),columns=['uniqueserver'])

    combined_network_df=cartesian_product_basic(uniqueservers_df,common_network_df)
    
    
    combined_network_df.Server=combined_network_df.uniqueserver
    combined_network_df.drop(['uniqueserver'],axis=1,inplace=True)
    combined_network_df = combined_network_df[['Server', 'Host', 
                       'MGMT Site 1', 'MGMT site1', 'MGMT Site 2', 'MGMT Site 2.1',
                       'BCK Site 1', 'Bck Site 2', 'App Site 1', 'App site 2', 'DB site 1',
                       'DB site 2', 'HSecurity1 Site 1', 'HSecurity2 Site 1',
                       'HSecurity1 Site 2', 'NAS Site 1(IP)', 'NAS site2 (IP)', 'App site 1',
                       'App site 2.1']]

    
    combined_network_df.fillna(value='',inplace=True)
    combined_network_df=combined_network_df.groupby('Server').aggregate(' '.join).reset_index()
    combined_final_df=server_network_df_agg.append(combined_network_df).groupby('Server').agg(' '.join).reset_index()
    combined_final_df['network_json']=combined_final_df.apply(lambda row :  eth_gen(row) ,axis=1)

    return combined_final_df


    


# In[8]:


def print_server_cfg(vm_df,network_df,req_servername):
  
    def print_server_cfg_disks(disklist):
        detinfo='Disks :'
        for disks in disklist["disklist"]:
            print(detinfo, disks['diskname'], disks['diskgb'])
            detinfo='       '
  
    def print_network_cfgs(ethlist):
        blankspaces='       '
        for eth in ethlist["network_info"]:
            print( blankspaces , "name : " , eth['name'])
            print( blankspaces , "hosetname : " , eth['hostname'])
            print( blankspaces , "ip : " , eth['ip'])
            print( blankspaces , "netmask : " , eth['netmask'])
            print( blankspaces , "gateway : " , eth['gateway'])
            print( blankspaces , "vlan : " , eth['vlan'])
            print("\n"  )
        
    vm_reqrow = vm_df[vm_df['VM Name'].str.lower() == req_servername.lower()]
    print ('Servername :' , vm_reqrow['VM Name'].values[0])
    print ( 'CPU :', vm_reqrow['vcpu'].apply(int).values[0])
    print ( 'Memory :', vm_reqrow['Memory (GB)'].apply(int).values[0])
    vm_reqrow['diskslist'].apply(lambda x: print_server_cfg_disks(x))
    for eth in ['eth0','eth1','eth2','eth3','eth4']:
        if  not vm_reqrow[eth].fillna('').values[0] == '' : 
            print ( eth + ' :' , vm_reqrow[eth].fillna('').values[0])
    
    print ("networks:")
    
    network_reqrow = network_df[network_df['Server'].str.lower() == req_servername.lower()]
    network_reqrow['network_json'].apply(lambda x: print_network_cfgs(x))


# In[ ]:





# In[9]:


def main():

    excel_file_name=sys.argv[1]
    server_sheet_name=sys.argv[2]
    network_sheet_name=sys.argv[3]
    server_name=sys.argv[4]
    
    global server_sheet_columns, network_sheet_columns
    
    server_sheet_columns=['x86 &VM Definition', 'Hardware Type', 'PCA', 'VM Name', 'vcpu',
                           'Memory (GB)', 'OS Disk', 'Repository', 'Data Disk',
                           'Data Disk Size (GB)', 'swap', 'Virtual', 'HA', 'eth0', 'eth1', 'eth2',
                           'eth3', 'eth4']
    
    network_sheet_columns=['Servers & Networks', 'Server', 'Host', 'Description', 'site',
                           'MGMT Site 1', 'MGMT site1', 'MGMT Site 2', 'MGMT Site 2.1',
                           'BCK Site 1', 'Bck Site 2', 'App Site 1', 'App site 2', 'DB site 1',
                           'DB site 2', 'HSecurity1 Site 1', 'HSecurity2 Site 1',
                           'HSecurity1 Site 2', 'NAS Site 1(IP)', 'NAS site2 (IP)', 'App site 1',
                           'App site 2.1']
    
    vmdefn_fromfile=read_server_defn(excel_file_name,server_sheet_name)
    vmdefn_jsondisk=disks_to_json(vmdefn_fromfile)

    network_defn_fromfile=read_server_network_defn(excel_file_name,network_sheet_name)
    network_common_defn_fromfile=read_common_network_defn(excel_file_name,network_sheet_name)

    networkdefn_jsonnw=network_to_json(network_defn_fromfile, network_common_defn_fromfile)
    
    print_server_cfg(vmdefn_jsondisk,networkdefn_jsonnw,server_name)


# In[10]:



if __name__ == '__main__': 
 main()

