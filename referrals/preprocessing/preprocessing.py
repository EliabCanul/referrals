import pandas as pd
from difflib import SequenceMatcher


#
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def ZCode(x):
    try:
        zc = 'zc-'+str(int(x))
        return zc
    except:
        return 'zc-nan'

def StoreID(x):
    try:
        zc = 'SId-'+str(int(x))
        return zc
    except:
        return 'SId-nan'

def Week(x):   
    try:
        zc = 'w-'+str(int(x))
        return zc
    except:
        return 'w-nan'

def check_shares(x, dshares):
    try:
        sh = dshares[x]
        return sh
    except:
        return 0


def map_cel2id(dict_referrer_client, x):
    try:
        r = dict_referrer_client[x]
        return r
    except:
        return 'None'



# Data
#ref_ref = pd.read_csv('./data/Referral_Coupons_Usage_Individual_Orders_EC_2022_02_10.csv')#[:80]

def process_data(ref_ref, save_outputs=False):
    """Make initial dataframes of referrals. This is the simplest processing
    to analize interactions and user properties.

    :param ref_ref: the dataframe (referrer-referral) coming from QUERY_REFERRALS
    :type ref_ref: dataframe
    :param save_outputs: A flag to save dataframes in the local directory. Defaults to False.
    :type save_outputs: bool, optional
    :return: a tuple of dataframes:
            [0]: An extended dataframe of the input one (ref_ref), containing new columns
            [1]: A dataframe of user's properties, one row per user
    :rtype: tuple
    """    
    
    # Dictionary referrer_num: referrer_name
    dict_referrers = dict(zip(ref_ref['Referrer_num'], ref_ref['Referrer_Name'] ))
    dict_referrers = { k:[v,'Intermediate user'] for k,v in dict_referrers.items()}

    # Dictionary referral_num: referral_name
    dict_referrals = dict(zip(ref_ref['Referral_num'], ref_ref['Referral_Name']))
    dict_referrals = { k:[v,'End user'] for k,v in dict_referrals.items()}

    # Change name to adapt to jaal using 'from' and 'to'
    ref_ref.rename(columns={'Referrer_num':'from', 'Referral_num':'to'}, inplace=True) #, 'StoreName':'label'
    ref_ref.drop(['GMV_after_discount'],axis=1,inplace=True)  #,'CouponCode','Referrer', 'OrderId'

    #

    # Make new column with unique interactions
    ref_ref['from-to'] = ref_ref['from'].astype(str) + '->' +ref_ref['to'].astype(str)

    # Calculates metric of similarity between referrer name and referral name
    #ref_ref['Name_similarity'] = ref_ref.apply(lambda x: similar(x['from'], x['to']), axis=1)
    ref_ref['name_similarity'] = ref_ref.apply(lambda x: similar(x['Referrer_Name'], x['Referral_Name']), axis=1)

    # Count number of repeated interactions
    fromto = ref_ref['from-to'].value_counts() # aqui se puede sacar histograma de cuantas veces se repitio la misma interacción
    dfromto = dict(fromto)

    # Make new column with number of repeated events
    ref_ref['n_interactions'] = ref_ref['from-to'].apply(lambda x: dfromto[x])

    # Number of shares
    shares = ref_ref['from'].value_counts()
    dshares = dict(shares)

    # Select repeated interactions
    group_same_interactions = ref_ref[ref_ref['n_interactions']>1].groupby('from-to')

    # Make a list of indexes to drop
    drop_these_ids = []
    for interaction, ginter in group_same_interactions:
        #print(interaction)
        ginter.sort_values(by=['OrderUTC'], inplace=True)
        #display(ginter[['OrderUTC','week']])
        # Remove all the repeated interactions except the last (in time)
        to_remove = list(ginter.index)[:-1]
        
        drop_these_ids.extend(to_remove)
        
    # Drop the rows with repeated interactions and leave only one
    ref_ref.drop(drop_these_ids, inplace=True)

    # Drop the 'from-to' column
    ref_ref.drop(['from-to'], axis=1, inplace=True)

    # Set to 'OLD' for repeated interactions
    to_be_old = ref_ref.loc[ref_ref['n_interactions']>1 ].index 
    ref_ref.loc[to_be_old, 'IsNewCustomer'] = 'OLD'

    # Add suffix to these columns:
    # make Zip Code string
    ref_ref['zip_code'] = ref_ref['ZipCode'].apply(lambda x: ZCode(x) )

    # make the Store Id string
    ref_ref['store_id'] = ref_ref['StoreId'].apply(lambda x: StoreID(x) )

    # make the week string
    ref_ref['week'] = ref_ref['week'].apply(lambda x: Week(x) )


    # Joining dictionaries: referrers and referrals
    dict_referrals.update(dict_referrers)

    # Data Frame with properties of nodes
    df_all_users = pd.DataFrame.from_dict(dict_referrals, orient='index')
    df_all_users.reset_index(inplace=True)
    df_all_users.columns = ['id', 'label', 'user_type']

    # Count number of shares per user
    df_all_users['n_shares'] = df_all_users['id'].apply(lambda x: check_shares(x,dshares) )

    # Sort
    ref_ref.sort_values('n_interactions', ascending=False, inplace=True)
    df_all_users.sort_values('n_shares',ascending=False, inplace=True)
    
    # Save df's
    if save_outputs:
        ref_ref.to_csv('../../data/referrer_referral.csv',index=False)
        df_all_users.to_csv('../../data/user_properties.csv',index=False)
    
    return ref_ref, df_all_users
