import pandas as pd
import numpy as np
import datetime
import pickle
import warnings
from netadata.ion.ion import query_rds, push_table
from ..ion import ion as ion 
from ..models import models as models
from ..preprocessing import preprocessing as prep
warnings.filterwarnings('ignore')
from dotenv import find_dotenv, load_dotenv

class Manager:
    ## Main class managers inherit from. DO NOT use this as template, use ManagerTemplate below
    log = ''
    version = (0,0,0)

    run_params = {
                    'name':'Baseline Manager',
                    'description':"""""",
                    'data' : {},
                    'IO':{},
                    'version':{},
                    
                            }


    def __init__(self) -> None:

        # Overwrite
        pass


    def run(self):
        # Overwrite
        return 

    def save_run_file(self, path : str):
        """ Saves runfile to path
        Args:
            path (str): [description]
        """

        self.run_params['savetime'] = datetime.datetime.now().strptime('$Y-$m-$d')
        self.run_params['version'] = self.version
        pickle.dump(self.run_params, open(path, 'wb'))
        

    def load_run_file(self, path : str):
        """Loads runfile to manager
        Args:
            path (str): path to file
        """
        self.run_params = pickle.load(self.run_params, open(path, 'rb'))




class ReferralsAnalysis(Manager): #Use this template to make your own experiment
    log = ''
    version = (0,1,1) # Everytime you change something in the code, please update this so we can keep track of changes
    run_params = {
                    'name':'Baseline Manager', # To identify between run files
                    'description':"""A class to get mutiple virality metrics. """,
                    'data' : {}, # Targets, features, models, etc....
                    'IO':{}, # Path to DBs used, functions to load / save data
                    'version':{}, # Keep track of different versions of the package in case of debug/reproducibility
                    'log':{}
                            }


    def __init__(self, run_params = None ) -> None:

        if run_params:
            self.run_params = run_params
        pass

    def run(self):
        """Run the pipeline for the referrals analysis. It generates multiple tables with
        virality metrics according to user stages and a ranking of influencers.
        
        """

        # IO: LOAD ----------------------------------------------------
        # Load connection credentials
        load_dotenv(find_dotenv())
        
        # --> Make queries to RDS. These will be used throughout the code
        
        # Query referrer-referral ocurrences
        results = query_rds('prod', query=ion.QUERY_REFERRALS, limit_1000=False)
        
        # Query of users behaviours
        self.users_behaviour = query_rds('prod', query=ion.QUERY_USERS_BEHAVIOR, limit_1000=False)

        # --> Preprocessed data
        ref_ref, users = prep.process_data(results)

        # Dictionary mapping cellphone and ClientId
        dict_referrer_client = dict(zip(ref_ref['from'], ref_ref['ClientId']))
        
        # Dictionary mapping cellphone and ClientId for the 'childrens'
        dict_child_cel2Id = dict(zip(ref_ref['to'], ref_ref['childId']))

        # Dictionary mapping cellphone to referral name
        #dict_to_referral = dict(zip(ref_ref['to'], ref_ref['Referral_Name']))


        # IO: Your Work -----------------------------------------------

        # Add ClientId column from cellphone
        users['ClientId'] = users['id'].apply(lambda x: prep.map_cel2id(dict_referrer_client, x) )

        # Split users data according to sharing
        sharing = users.n_shares != 0
        users_share = users[sharing]
        users_not_share = users[~sharing]
        users_not_share['ClientId'] = users_not_share['id'].apply(lambda x: dict_child_cel2Id[x])

        # --> Determine potential influencers: those sharing more than the mean
        inf_pot = users_share[users_share.n_shares > users_share['n_shares'].mean()]
        
        # Get the interactions of potential influencers
        # Class variable
        self.inf_pot_interactions = ref_ref[ref_ref['ClientId'].isin(inf_pot['ClientId'])]

        # --> Calculate many virality metrics
        # Count the number of different people
        inf_pot['n_distinct_users'] = inf_pot['ClientId'].apply(lambda x: self.count_distinct_users(x))

        # Mean shares per user
        inf_pot['mean_shares_per_user'] = inf_pot['n_shares']/inf_pot['n_distinct_users']
        
        # TODO: Check user's virality criteria 
        us_min = inf_pot['n_distinct_users'].min()
        us_max = inf_pot['n_distinct_users'].max()        
        inf_pot['vir_users'] = inf_pot['n_distinct_users'].apply(lambda x: (x-us_min)/(us_max-us_min))

        # Used coupon
        inf_pot['coupon'] = inf_pot['ClientId'].apply(lambda x: self.which_coupon_used(x))

        # Number of ZC where the coupons are used
        inf_pot['delivery_zipcodes'] = inf_pot['ClientId'].apply(lambda x: len(self.which_ZC(x)))

        # Number of stores where the coupons are used
        inf_pot['delivery_stores'] = inf_pot['ClientId'].apply(lambda x: len(self.which_store(x)))

        # Physical virality
        w1 = 1.0
        w2 = 0.8
        inf_pot['vir_physical'] = w1*inf_pot['delivery_zipcodes'] + w2*inf_pot['delivery_stores']
        fis_min = inf_pot['vir_physical'].min()
        fis_max = inf_pot['vir_physical'].max()
        inf_pot['vir_physical'] = inf_pot['vir_physical'].apply(lambda x: (x-fis_min)/(fis_max-fis_min))
        
        # List of active weeks
        #inf_pot['active_weeks'] = inf_pot['ClientId'].apply(lambda x: self.which_weeks(x))
        
        # Number of active weeks
        inf_pot['n_active_weeks'] = inf_pot['ClientId'].apply(lambda x: len(self.which_weeks(x)))

        # Temporal virality
        tmp_min = inf_pot['n_active_weeks'].min()
        tmp_max = inf_pot['n_active_weeks'].max()
        inf_pot['vir_temp'] = inf_pot['n_active_weeks'].apply(lambda x: (x-tmp_min)/(tmp_max-tmp_min))

        # --> K-factor
        inf_pot['k_factor'] = inf_pot['ClientId'].apply(lambda x: models.calculate_K_factor(x, self.inf_pot_interactions))

        # 
        self.ref_ref = ref_ref

        # Get metrics of parent and childrens
        the_parent, the_children = self.get_users_metrics()

        #
        potential_influencers = pd.merge(inf_pot, the_parent, left_on='ClientId', right_on='ClientId')

        # get_influncers_ranking ??
        """influencers = potential_influencers[['ClientId','label','coupon','id',
                                             'k_factor','active','vir_users',
                                             'vir_physical','vir_temp','child_sharing_rate',
                                             'active_child_fracc']]
        
        influencers['virality_score'] = influencers['active'] + influencers['vir_users'] + influencers['vir_physical'] + \
                                    influencers['vir_temp'] + influencers['child_sharing_rate'] + \
                                    influencers['active_child_fracc']
        influencers.sort_values('virality_score', ascending=False, inplace=True, ignore_index=True)"""
        

        potential_influencers['virality_score'] = potential_influencers['active'] + potential_influencers['vir_users'] + potential_influencers['vir_physical'] + \
                                    potential_influencers['vir_temp'] + potential_influencers['child_sharing_rate'] + \
                                    potential_influencers['active_child_fracc']
        potential_influencers.sort_values('virality_score', ascending=False, inplace=True, ignore_index=True)        
        
        
        # --> Users in stage 3: Potential influencer interactions
        self.inf_pot_interactions.reset_index(drop=True,inplace=True)


        # --> Users in stage 2: Intermediate users. 
        
        # users sharing less than the sharing mean rate
        intermediate_users = users_share[users_share.n_shares <= users_share['n_shares'].mean()]
        intermediate_users['active'] = intermediate_users['ClientId'].apply(lambda x: self.get_means_df(x, 'activo1'))
        intermediate_users['mean_freq'] = intermediate_users['ClientId'].apply(lambda x: self.get_means_df(x, 'frecuencia'))
        intermediate_users['mean_gmv'] = intermediate_users['ClientId'].apply(lambda x: self.get_means_df(x, 'gmv'))
        intermediate_users['mean_AOV'] = intermediate_users['ClientId'].apply(lambda x: self.get_means_df(x, 'AOV'))
        intermediate_users.sort_values('mean_AOV',ascending=False,inplace=True)
        intermediate_users.reset_index(drop=True,inplace=True)
        
        # Interactions of users in stage 2
        intermediate_user_interactions = ref_ref[ref_ref['ClientId'].isin(intermediate_users['ClientId'])]
        intermediate_user_interactions.reset_index(drop=True,inplace=True)
        intermediate_user_interactions

        # --> Users  in stage 1: users not sharing
        #
        users_not_share['active'] = users_not_share['ClientId'].apply(lambda x: self.get_means_df(x, 'activo1'))
        users_not_share['mean_freq'] = users_not_share['ClientId'].apply(lambda x: self.get_means_df(x, 'frecuencia'))
        users_not_share['mean_gmv'] = users_not_share['ClientId'].apply(lambda x: self.get_means_df(x, 'gmv'))
        users_not_share['mean_AOV'] = users_not_share['ClientId'].apply(lambda x: self.get_means_df(x, 'AOV'))

        users_not_share.sort_values('mean_AOV',ascending=False, inplace=True)
        users_not_share.reset_index(drop=True,inplace=True)
        
        
        # TODO: Change column names in ION
        # --> Final columns renaming (for redability) and dropping 
        # use snake case format
        #
        users_not_share.rename(columns={'id':'phone_number',
                                        'label':'user_name',
                                        'ClientId':'client_id',
                                        'mean_AOV':'mean_aov'
                                        }, inplace=True)
        users_not_share.drop(['user_type'], axis=1, inplace=True)
        users_not_share['user_type'] = 'passive'
        
        #
        intermediate_users.rename(columns={'id':'phone_number',
                                           'label':'user_name',
                                           'ClientId':'client_id',
                                           'mean_AOV':'mean_aov'
                                        }, inplace=True)
        intermediate_users.drop(['user_type'], axis=1, inplace=True)
        intermediate_users['user_type'] = 'intermediate'
        
        #
        intermediate_user_interactions.rename(columns={'OrderId':'order_id',
                                                       'OrderUTC':'order_utc',
                                                       'CouponCode':'coupon_code',
                                                       'Referrer_Name': 'referrer_name',
                                                       'Referral_Name':'referral_name',
                                                       'IsNewCustomer':'is_new_customer',
                                                       'StoreName':'store_name',
                                                       'StoreId':'store_id',
                                                       'ZipCode':'zipcode',
                                                       'ClientId':'referrer_client_id',
                                                       'childId':'referral_client_id',
                                                       'from': 'referrer_phone_number',
                                                       'to': 'referral_phone_number'           
                                                       }, inplace=True)
        intermediate_user_interactions['referrer_type'] = 'intermediate'
        
        #
        potential_influencers.rename(columns={'id':'phone_number',
                                              'label':'user_name',
                                              'ClientId':'client_id',
                                              'mean_AOV':'mean_aov'
                                        }, inplace=True)
        potential_influencers.drop(['user_type'], axis=1, inplace=True)
        potential_influencers['user_type'] = 'influencer'
        
        #
        self.inf_pot_interactions.rename(columns={'OrderId':'order_id',
                                                  'OrderUTC':'order_utc',
                                                  'CouponCode':'coupon_code',
                                                  'Referrer_Name': 'referrer_name',
                                                  'Referral_Name':'referral_name',
                                                  'IsNewCustomer':'is_new_customer',
                                                  'StoreName':'store_name',
                                                  'StoreId':'store_id',
                                                  'ZipCode':'zipcode',
                                                  'ClientId':'referrer_client_id',
                                                  'childId':'referral_client_id',
                                                  'from': 'referrer_phone_number',
                                                  'to': 'referral_phone_number'           
                                                       }, inplace=True)     
        self.inf_pot_interactions['referrer_type'] = 'influencer'
        

        # IO: OUT -----------------------------------------------------
        
        # etapa 1: not_share users_not_share.to_csv('./data/users_not_share.csv')
        # etapa 2: intermediate_users intermediate_users.to_csv('./data/intermediate_users.csv')
        # etapa 3: influencers

        user_stages = pd.concat([potential_influencers, intermediate_users, users_not_share])
        user_stages.reset_index(drop=True, inplace=True)
        
        user_interactions = pd.concat([self.inf_pot_interactions, intermediate_user_interactions])
        user_interactions.reset_index(drop=True, inplace=True)
        

        # -- > Push tables to rds
        
        push_table(user_stages, 'referrals_user_stages', 'data', if_exists = 'replace') 

        push_table(user_interactions, 'referrals_user_interactions', 'data', if_exists = 'replace')

        return user_stages, user_interactions
        

    def count_distinct_users(self,x):
        n = len(self.inf_pot_interactions.groupby('ClientId').get_group(x))
        return n

    def which_coupon_used(self,x):
        cupon = self.inf_pot_interactions.groupby('ClientId').get_group(x)['CouponCode'].unique()
        if len(cupon)==1:
            cupon = cupon[0]
        else:
            raise f"{x} has more than one coupon! Check!."
        return cupon

    def which_ZC(self,x):
        zc = self.inf_pot_interactions.groupby('ClientId').get_group(x)['ZipCode'].unique()
        return zc

    def which_store(self,x):
        store = self.inf_pot_interactions.groupby('ClientId').get_group(x)['StoreName'].unique()
        return store

    def which_weeks(self,x):
        weeks = [w.split('-')[1] for w in self.inf_pot_interactions.groupby('ClientId').get_group(x)['week'].unique().tolist()]
        return weeks
    
    def shares_by_week(self,x):
        #Count the number of shares by week
        tmp = pd.DataFrame(data=None, columns=['ClientId','week', 'n_users'])
        i=0
        client = self.inf_pot_interactions.groupby('ClientId').get_group(x)
        for week, wgroup in client.groupby('week'):
            row = [x, week.split('-')[1], len(wgroup['week'])]
            tmp.loc[i] = row
            i+=1
        tmp = tmp.sort_values('week')
        tmp.reset_index(drop=True, inplace=True)
        return tmp
    
    def get_users_metrics(self, verbose=False):
            
        the_parent = pd.DataFrame(data=None, columns=['ClientId','active','mean_freq',
                                                    'mean_gmv','mean_AOV','child_sharing_rate',
                                                    'active_child_fracc'
                                                    ])
        the_children = pd.DataFrame(data=None)
        
        c = 0
        # Itera sobre Cient Ids de Influencers potenciales
        for cid, inf_group in self.inf_pot_interactions.groupby('ClientId'): 
            
            # Parents (Influencers potenciales originales)
            parent_cel = inf_group['from'].unique()[0]
            parent_name = inf_group['Referrer_Name'].unique()
            auto_interacciones = inf_group[inf_group['from']==inf_group['to']]['n_interactions'].values
            
            if len(auto_interacciones)==0:
                auto_interacciones = 0
            else:
                auto_interacciones = auto_interacciones[0]
            
            # children son numeros de celular unicos (deben ser childId)
            children = list(inf_group['to'].unique() )
            children_ids = list(inf_group['childId'].unique())

            df_children_that_share = self.ref_ref[self.ref_ref['from'].isin(children)]
            
            # children share to parent?
            parent_in_child = parent_cel in children
            
            if parent_in_child:
                # Remove the contribution from parent
                n_child = len(children) - 1
                n_child_share = len(df_children_that_share['ClientId'].unique()) - 1 # count_children_sharing(children) - 1
                # Remove parent ids from lists so information is not repeated
                children.remove(parent_cel)
                children_ids.remove(cid)
            else:
                n_child = len(children)
                n_child_share = len(df_children_that_share['ClientId'].unique()) # count_children_sharing(children)

            
            children_sharing_rate =  round(n_child_share*100/n_child, 2)
            
            #
            if verbose:
                print(cid, parent_name, parent_cel)
                print("Auto interactions: ", auto_interacciones)
                print("Parent in children: ", parent_in_child)                    
                print(children) 
                print("Ids de hijos: ",  children_ids) 
                #
                print("n children :",n_child)
                print("how many children share?:", n_child_share)
                print("sharing rate: ",children_sharing_rate, "%" )
                

            # ========== Tabla de comportamiento de los hijos
            children_behavior = self.users_behaviour[self.users_behaviour['cliente'].isin( children_ids )]
            
            the_children_tmp = pd.DataFrame(data=None)
            
            the_children_tmp['active'] = children_behavior.groupby('cliente')['activo1'].mean()
            the_children_tmp['mean_freq'] = children_behavior.groupby('cliente')['frecuencia'].mean()
            the_children_tmp['mean_gmv'] = children_behavior.groupby('cliente')['gmv'].mean()    
            the_children_tmp['mean_AOV'] = children_behavior.groupby('cliente')['AOV'].mean()    
            the_children_tmp['parent_Id'] = cid
        
            the_children = pd.concat([the_children, the_children_tmp])

            # ========= Analisis del comportamiento del padre e inclusión de información de hijos
            parent_behavior = self.users_behaviour[self.users_behaviour['cliente'] == cid ]
            the_parent.loc[c] = [cid,
                            parent_behavior['activo1'].mean(),
                            parent_behavior['frecuencia'].mean(),
                            parent_behavior['gmv'].mean(),
                            parent_behavior['AOV'].mean(),
                            children_sharing_rate/100.,
                            # 
                            children_behavior.groupby('cliente')['activo1'].mean().mean()
                            ]    
            
            c += 1

        the_parent = the_parent.astype({'ClientId':int})

        return the_parent, the_children

    def get_means_df(self, ClientId, col='activo1'):
    
        return self.users_behaviour[self.users_behaviour.cliente==ClientId][col].mean()