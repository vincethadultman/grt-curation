import pandas as pd
import numpy as np
import json
import pytz
import datetime

import jobs

def get_df(raw, type):
    
    print("Constructing dataframe...")
    
    df = pd.DataFrame()
    
    if type == 'allocations':
        df = pd.DataFrame()
        
        for block, data in raw.items():
            df_block = pd.json_normalize(data, sep='_')
            df_block['block'] = block
            
            df = pd.concat([df, df_block], sort=False)
        
        json_struct = json.loads(df.explode('subgraphDeployment_versions').to_json(orient="records"))    
        df = pd.json_normalize(json_struct, sep='_') #use pd.io.json
    
    else:

        for block, data in raw.items():
            
            df_block = pd.json_normalize(data, sep='_')
            df_block['block'] = block
            
            df = pd.concat([df, df_block], sort=False)

    return df

def clean_names(df, query):
    
    print("Cleaning column names...")

    return df.rename(columns = jobs.columns_in[query])

def clean_data(df, query):
    
    print("Cleaning data formats...")
    
    if query == 'main':

        df['created_at'] = pd.to_datetime(df['created_at'],unit='s')
        df['created_date'] = df['created_at'].dt.date
        df['signal'] = df['signal'].astype(float)/10**18
        df['curator_rewards_total'] = df['curator_rewards_total'].astype(float)/10**18
        df['indexer_rewards_total'] = df['indexer_rewards_total'].astype(float)/10**18
        df['delegator_rewards_total'] = df['delegator_rewards_total'].astype(float)/10**18
        df['QF_total'] = df['QF_total'].astype(float)/10**18
        df['shares'] = df['shares'].astype(float)/10**18
        df['stake'] = df['stake'].astype(float)/10**18
        df['price_per_share'] = df['price_per_share'].astype(float)
        
    elif query == 'global':
        
        df['stake'] = df['stake'].astype(float)/10**18
        df['delegation'] = df['delegation'].astype(float)/10**18
        df['allocation'] = df['allocation'].astype(float)/10**18
        df['QF_total'] = df['QF_total'].astype(float)/10**18
        df['curator_rewards_total'] = df['curator_rewards_total'].astype(float)/10**18
        df['indexer_rewards_total'] = df['indexer_rewards_total'].astype(float)/10**18
        df['delegator_rewards_total'] = df['delegator_rewards_total'].astype(float)/10**18
        df['signal'] = df['signal'].astype(float)/10**18 
        
    elif query == 'allocations':
        
        df['allocated_tokens'] = df['allocated_tokens'].astype(float)/10**18
        df['closed_at_block'] = df['closed_at_block'].fillna(0).astype(int)
        df['delegation_fees'] = df['delegation_fees'].astype(float)/10**18
        df['QF_collected'] = df['QF_collected'].astype(float)/10**18

    return df

def add_blocktimes(df, blocktimes):
    
    print("Adding blocktimes...")
    
    df_blocktimes = pd.DataFrame(data=blocktimes, columns=['block', 'blockTime'])
    
    df = df.merge(df_blocktimes, on='block')
    #df['blockDatetime'] = pd.to_datetime(df['blockTime'],unit='s')
    df['date'] = pd.to_datetime(df['blockTime'],unit='s').dt.date
    
    return df

def rename(df, type):
    
    return df.rename(columns = jobs.rename[type])

def aggregate(df, type):
    
    print("Aggregating dataframe...")
    
    if type == 'global':
        pass
    
    elif type == 'subgraphs':
        
        df_names = df[['display_name', 'subgraph_id']].drop_duplicates().copy().reset_index(drop=True)
        
        df = (df.groupby(['subgraph_id','block','date', 'created_at'])
              .agg(
                  {
                      'active' : 'max',
                      'curator_rewards_total': 'sum',
                      'delegator_rewards_total': 'sum',
                      'indexer_rewards_total': 'sum',
                      'price_per_share' : 'max',
                      'QF_total': 'sum',
                      'shares' : 'max',
                      'signal': 'sum',
                      'stake': 'sum'
                  }
              )
              .reset_index()
              .copy())
        
        df = df.merge(df_names, on=['subgraph_id'])
    
    return df

        
def add_columns(df, type):
    
    print("Adding new columns...")
    
    if type == 'allocations':
        df['subgraph_id'] = df.apply(lambda x: x['version_id'][:-2] if x['version_id'] else None, axis = 1)
    
    elif type == 'global':
        
        # Adding diffs
        df['QF'] = df['QF_total'].diff().fillna(0)
        
        df['signal_change'] = df['signal'].diff().fillna(0)
        df['signal_change_7d'] = df['signal'].diff(periods=7).fillna(0)
        df['signal_change_30d'] = df['signal'].diff(periods=30).fillna(0)
        
        df['stake_change'] = df['stake'].diff().fillna(0)
        df['stake_change_7d'] = df['stake'].diff(periods=7).fillna(0)
        df['stake_change_30d'] = df['stake'].diff(periods=30).fillna(0)
        
        df['allocation_change'] = df['allocation'].diff().fillna(0)
        df['allocation_change_7d'] = df['allocation'].diff(periods=7).fillna(0)
        df['allocation_change_30d'] = df['allocation'].diff(periods=30).fillna(0)
        
        df['delegation_change'] = df['delegation'].diff().fillna(0)
        df['delegation_change_7d'] = df['delegation'].diff(periods=7).fillna(0)
        df['delegation_change_30d'] = df['delegation'].diff(periods=30).fillna(0)
        
        df['curator_rewards'] = df['curator_rewards_total'].diff().fillna(0)
        df['indexer_rewards'] = df['indexer_rewards_total'].diff().fillna(0)
        df['delegator_rewards'] = df['delegator_rewards_total'].diff().fillna(0)
        
        # Adding moving sums and averages (remember to sort!)
        df = df.sort_values(['date'], ascending=True).reset_index()
        
        df['QF_7d'] = df['QF'].rolling(7).sum()
        df['QF_30d'] = df['QF'].rolling(30).sum()
        df['curator_rewards_7d'] = df['curator_rewards'].rolling(7).sum()
        df['curator_rewards_30d'] = df['curator_rewards'].rolling(30).sum()
        df['indexer_rewards_7d'] = df['indexer_rewards'].rolling(7).sum()
        df['indexer_rewards_30d'] = df['indexer_rewards'].rolling(30).sum()
        df['delegator_rewards_7d'] = df['delegator_rewards'].rolling(7).sum()
        df['delegator_rewards_30d'] = df['delegator_rewards'].rolling(30).sum()
        
        # Arithmetic
        df['signal_per_stake'] = df['signal'] / df['allocation']
        df['QF30D_per_signal'] = df['QF_30d'] / df['signal']
        
        # APR estimates        
        df_feesplit = pd.read_csv('outputs/allocations.csv')
        feesplit = df_feesplit['delegation_fees'].sum()/df_feesplit['QF_collected'].sum()
        
        df['curator_apr_30d_estimate'] = (df['curator_rewards_30d'] / df['signal'].rolling(30).mean()) * 12
        df['indexer_apr_30d_estimate'] = ((df['indexer_rewards_30d'] + (1-feesplit)* df['QF_30d']) / df['stake'].rolling(30).mean()) * 12
        df['delegator_apr_30d_estimate'] = ((df['delegator_rewards_30d'] + feesplit* df['QF_30d'])/ df['delegation'].rolling(30).mean()) * 12
        
    if type == 'subgraphs':
        
        df = df.sort_values(['subgraph_id', 'block'])
        
        # Adding diffs per subgraph per block interval

        df['QF'] = df.groupby('subgraph_id')['QF_total'].diff().fillna(0)
        df['signal_change'] = df.groupby('subgraph_id')['signal'].diff().fillna(0)
        df['stake_change'] = df.groupby('subgraph_id')['stake'].diff().fillna(0)
        df['shares_change'] = df.groupby('subgraph_id')['shares'].diff().fillna(0)
        df['price_change'] = df.groupby('subgraph_id')['price_per_share'].diff().fillna(0)
        df['curator_rewards'] = df.groupby('subgraph_id')['curator_rewards_total'].diff().fillna(0)
        df['indexer_rewards'] = df.groupby('subgraph_id')['indexer_rewards_total'].diff().fillna(0)
        df['delegator_rewards'] = df.groupby('subgraph_id')['delegator_rewards_total'].diff().fillna(0)
        
        # Rolling means and sums
        df['QF_7d'] = df.groupby('subgraph_id').rolling(7)['QF'].sum().reset_index(drop=True)
        df['QF_14d'] = df.groupby('subgraph_id').rolling(14)['QF'].sum().reset_index(drop=True)
        df['QF_30d'] = df.groupby('subgraph_id').rolling(30)['QF'].sum().reset_index(drop=True)
        
        df['signal_change_7d'] = df.groupby('subgraph_id').rolling(7)['signal_change'].sum().reset_index(drop=True)
        df['signal_change_14d'] = df.groupby('subgraph_id').rolling(14)['signal_change'].sum().reset_index(drop=True)
        df['signal_change_30d'] = df.groupby('subgraph_id').rolling(30)['signal_change'].sum().reset_index(drop=True)
        
        df['signal_7d_MA'] = df.groupby('subgraph_id').rolling(7)['signal'].mean().reset_index(drop=True)
        df['signal_14d_MA'] = df.groupby('subgraph_id').rolling(14)['signal'].mean().reset_index(drop=True)
        df['signal_30d_MA'] = df.groupby('subgraph_id').rolling(30)['signal'].mean().reset_index(drop=True)
        
        df['stake_change_7d'] = df.groupby('subgraph_id').rolling(7)['stake_change'].sum().reset_index(drop=True)
        df['stake_change_14d'] = df.groupby('subgraph_id').rolling(14)['stake_change'].sum().reset_index(drop=True)
        df['stake_change_30d'] = df.groupby('subgraph_id').rolling(30)['stake_change'].sum().reset_index(drop=True)
        
        df['stake_7d_MA'] = df.groupby('subgraph_id').rolling(7)['stake'].mean().reset_index(drop=True)
        df['stake_14d_MA'] = df.groupby('subgraph_id').rolling(14)['stake'].mean().reset_index(drop=True)
        df['stake_30d_MA'] = df.groupby('subgraph_id').rolling(30)['stake'].mean().reset_index(drop=True)
        
        df['shares_change_7d'] = df.groupby('subgraph_id').rolling(7)['shares_change'].sum().reset_index(drop=True)
        df['shares_change_14d'] = df.groupby('subgraph_id').rolling(14)['shares_change'].sum().reset_index(drop=True)
        df['shares_change_30d'] = df.groupby('subgraph_id').rolling(30)['shares_change'].sum().reset_index(drop=True)
        
        df['shares_7d_MA'] = df.groupby('subgraph_id').rolling(7)['shares'].mean().reset_index(drop=True)
        df['shares_14d_MA'] = df.groupby('subgraph_id').rolling(14)['shares'].mean().reset_index(drop=True)
        df['shares_30d_MA'] = df.groupby('subgraph_id').rolling(30)['shares'].mean().reset_index(drop=True)
        
        df['price_change_7d'] = df.groupby('subgraph_id').rolling(7)['price_change'].sum().reset_index(drop=True)
        df['price_change_14d'] = df.groupby('subgraph_id').rolling(14)['price_change'].sum().reset_index(drop=True)
        df['price_change_30d'] = df.groupby('subgraph_id').rolling(30)['price_change'].sum().reset_index(drop=True)
        
        df['price_7d_MA'] = df.groupby('subgraph_id').rolling(7)['price_per_share'].mean().reset_index(drop=True)
        df['price_14d_MA'] = df.groupby('subgraph_id').rolling(14)['price_per_share'].mean().reset_index(drop=True)
        df['price_30d_MA'] = df.groupby('subgraph_id').rolling(30)['price_per_share'].mean().reset_index(drop=True)
        
        df['curator_rewards_7d'] = df.groupby('subgraph_id').rolling(7)['curator_rewards'].sum().reset_index(drop=True)
        df['curator_rewards_14d'] = df.groupby('subgraph_id').rolling(14)['curator_rewards'].sum().reset_index(drop=True)
        df['curator_rewards_30d'] = df.groupby('subgraph_id').rolling(30)['curator_rewards'].sum().reset_index(drop=True)
        
        df['indexer_rewards_7d'] = df.groupby('subgraph_id').rolling(7)['indexer_rewards'].sum().reset_index(drop=True)
        df['indexer_rewards_14d'] = df.groupby('subgraph_id').rolling(14)['indexer_rewards'].sum().reset_index(drop=True)
        df['indexer_rewards_30d'] = df.groupby('subgraph_id').rolling(30)['indexer_rewards'].sum().reset_index(drop=True)
        
        df['delegator_rewards_7d'] = df.groupby('subgraph_id').rolling(7)['delegator_rewards'].sum().reset_index(drop=True)
        df['delegator_rewards_14d'] = df.groupby('subgraph_id').rolling(14)['delegator_rewards'].sum().reset_index(drop=True)
        df['delegator_rewards_30d'] = df.groupby('subgraph_id').rolling(30)['delegator_rewards'].sum().reset_index(drop=True)
        
        # Rates
        
        df['signal_per_stake'] = df['signal'] / df['stake']
        
        df['curator_rewards_per_signal_7d'] = df['curator_rewards_7d'] / df['signal_7d_MA']
        df['curator_rewards_per_signal_14d'] = df['curator_rewards_14d'] / df['signal_14d_MA']
        df['curator_rewards_per_signal_30d'] = df['curator_rewards_30d'] / df['signal_30d_MA']
        
        df['indexer_rewards_per_stake_7d'] = df['indexer_rewards_7d'] / df['stake_7d_MA']
        df['indexer_rewards_per_stake_14d'] = df['indexer_rewards_14d'] / df['stake_14d_MA']
        df['indexer_rewards_per_stake_30d'] = df['indexer_rewards_30d'] / df['stake_30d_MA']
        
        df['curator_apr_30d_estimate'] = df['curator_rewards_per_signal_30d'] * 12
        
        # Estimating indexer/delegator fee splits
        df_feesplit = pd.read_csv('outputs/allocations.csv')
        feesplit = df_feesplit['delegation_fees'].sum()/df_feesplit['QF_collected'].sum()
        
        df['indexer_apr_30d_estimate'] = (df['QF_30d'] * (1-feesplit) +  df['indexer_rewards_30d']) / df['stake_30d_MA'] * 12
        
        df['price_growth_7d'] = -df['price_per_share'] / (df['price_change_7d'] - df['price_per_share']) - 1
        df['price_growth_14d'] = -df['price_per_share'] / (df['price_change_14d'] - df['price_per_share']) - 1
        df['price_growth_30d'] = -df['price_per_share'] / (df['price_change_30d'] - df['price_per_share']) - 1
        
        df['curator_rewards_per_share_30d'] = df['curator_rewards_30d'] / df['shares_30d_MA']
        #df['curator_rewards_per_share_squared_30d'] = df['curator_rewards_30d'] / (df['shares_30d_MA']*df['shares_30d_MA'])
        #df['price_per_reward_30d'] = df['price_per_share'] / df['curator_rewards_per_share_30d']
        
    
    df = df.replace([np.inf, -np.inf], np.nan)
        
    return df

def drop_rows(df, type):
    
    if type == 'allocations':
        
        #Drop all rows where closed_block is outside of the block interval
        df = df.loc[(df['closed_at_block'] >= df['block'].min()) & (df['closed_at_block'] <= df['block'].max())]
    
    return df
        

def pipeline(raw, blocktimes, job):
    
    if job.type == 'allocations':
        df = get_df(raw, job.type)
        df = clean_names(df, job.query)
        df = clean_data(df, job.query)
        df = add_columns(df, job.type)
        df = drop_rows(df, job.type)
        
    elif job.type == 'global':
        df = get_df(raw, job.type)
        df = clean_names(df, job.query)
        df = clean_data(df, job.query)
        df = add_blocktimes(df, blocktimes)
        df = add_columns(df, job.type)
        
    elif job.type == 'subgraphs':
        df = get_df(raw, job.type)
        df = clean_names(df, job.query)
        df = clean_data(df, job.query)
        df = add_blocktimes(df, blocktimes)
        
        df = aggregate(df, job.type)
        df = add_columns(df, job.type)
        #print(df.columns)
        #return None
        
    print("Saving local file...")
        
    if job.type == 'allocations':
        
        #filedate = pytz.utc.localize(datetime.datetime.utcnow()).date().strftime('%m_%d_%Y')
        filename = f"{job.filename}.csv"
        df.to_csv(f'{job.output}/{filename}', columns=jobs.columns_out[job.type])
    
    else:
        filedate = df['date'].max().strftime('%m_%d_%Y')
        filename = f"{job.filename}_{filedate}.csv"

        (
            df
            .sort_values('date', ascending=True)
            .tail(job.rows).reset_index(drop=True)
            .to_csv(f'{job.output}/{filename}', columns=jobs.columns_out[job.type])
            )
    
    return filename, df