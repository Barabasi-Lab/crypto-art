######
# Author: Kishore Vasan
# this file loads and organizes
# Foundation data for further use
######

import numpy as np
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime,timedelta
from tqdm import tqdm
import matplotlib.gridspec as gridspec
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

import copy

def load_data():
  print("loading data...")
  art_metadata_df = pd.read_csv("../data/nft_metadata.csv")

  bidding_df = pd.read_csv("../data/bid_data.csv")
  bidding_df['bidding_dt'] = bidding_df.bid_date.apply(lambda x: datetime.utcfromtimestamp(int(x)))
  bidding_df['bidding_d'] = bidding_df.bidding_dt.apply(lambda x: x.date())

  listing_df = pd.read_csv("../data/list_data.csv")
  listing_df['list_dt'] = listing_df.listing_date.apply(lambda x: datetime.utcfromtimestamp(int(x)))
  listing_df['listing_d'] = listing_df.list_dt.apply(lambda x: x.date())

  minting_df = pd.read_csv("../data/mint_data.csv")
  minting_df['mint_dt'] = minting_df.mint_date.apply(lambda x: datetime.utcfromtimestamp(int(x)))
  minting_df['minting_date'] = minting_df.mint_dt.apply(lambda x: x.date())

  daily_ether_price = pd.read_csv("../data/daily-usd-ether-data.csv")
  daily_ether_price['d'] = daily_ether_price['Date(UTC)'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y'))
  daily_ether_price['d'] = daily_ether_price.d.apply(lambda x: x.date())

  # map ether price

  bidding_df = pd.merge(bidding_df, daily_ether_price[['Value','d']], how='left', left_on='bidding_d',
        right_on='d')
  bidding_df['cost_at_time'] = bidding_df.bidding_amt*bidding_df.Value
  bidding_df = bidding_df[['token_id','bidding_amt','bid_date','bidding_dt','bidding_d',
                           'creator','bidder_id','cost_at_time']]

  listing_df = pd.merge(listing_df, daily_ether_price[['Value','d']], how='left', left_on='listing_d',
        right_on='d')
  listing_df['cost_at_time'] = listing_df.listing_amt*listing_df.Value
  listing_df = listing_df[['token_id','listing_amt','creator',
                          'listing_date','list_dt','listing_d','cost_at_time']]

  ## reselling
  second_list_dt = []
  token_id_list = []

  for i in tqdm(listing_df.token_id.unique()):
      if len(listing_df[listing_df.token_id == i].list_dt.tolist()) >1:
          t = listing_df[listing_df.token_id == i].sort_values('list_dt')
          second_list_dt.append(t.list_dt.tolist()[1])
          token_id_list.append(i)

  second_list_df = pd.DataFrame({'token_id':token_id_list,'second_list_dt':second_list_dt})
  print("N art re-listed:", second_list_df.token_id.nunique())

  tmp = pd.merge(bidding_df, second_list_df, how='left',on='token_id')
  tmp['second_list_dt'] = tmp.second_list_dt.fillna(datetime.strptime('2021-9-28','%Y-%m-%d'))

  print("N art resold:", tmp[tmp.bidding_dt >tmp.second_list_dt].token_id.nunique())
  tmp = tmp[tmp.bidding_dt < tmp.second_list_dt]

  # retain the primary art bids
  bidding_df = copy.deepcopy(tmp)

  ####
  # note the listing also shows re-selling dates
  # for this we only filter the primary market
  ####

  first_list_df = listing_df.groupby('token_id').list_dt.min().to_frame()
  first_list_df['token_id'] = first_list_df.index
  first_list_df.index = range(len(first_list_df))
  first_list_df = pd.merge(first_list_df,listing_df, how='left', on=['token_id','list_dt'])

  # retain the first listing date
  listing_df = copy.deepcopy(first_list_df)

  final_bid_df = bidding_df.groupby('token_id').bidding_dt.max().to_frame()
  final_bid_df['token_id'] = final_bid_df.index
  final_bid_df.index = range(len(final_bid_df))
  final_bid_df['final_d_bidding'] = final_bid_df.bidding_dt.apply(lambda x: x.date())
  final_bid_df['final_t_bidding'] = final_bid_df.bidding_dt.apply(lambda x: datetime.strftime(x, '%H:%M'))
  final_bid_df.columns = ['final_dt_bidding','token_id','final_d_bidding','final_t_bidding']

  max_bid_df = bidding_df.groupby('token_id').cost_at_time.max().to_frame()
  max_bid_df['art'] = max_bid_df.index
  max_bid_df.index = range(len(max_bid_df))
  max_bid_df.columns = ['selling_price_usd','token_id']

  max_bid_df_2 = bidding_df.groupby('token_id').bidding_amt.max().to_frame()
  max_bid_df_2['art'] = max_bid_df_2.index
  max_bid_df_2.index = range(len(max_bid_df_2))
  max_bid_df_2.columns = ['selling_price_eth','token_id']

  max_bid_df = pd.merge(max_bid_df, max_bid_df_2, how='inner')

  max_bid_df = pd.merge(max_bid_df, listing_df[['token_id','list_dt','creator']])
  max_bid_df = pd.merge(max_bid_df, final_bid_df[['token_id','final_t_bidding','final_d_bidding','final_dt_bidding']])

  # add the owner info
  t = bidding_df[['token_id','bidder_id','bidding_dt']]
  t.columns = ['token_id','bidder_id','final_dt_bidding']

  max_bid_df = pd.merge(max_bid_df, t, how='left',on=['token_id','final_dt_bidding'])
  max_bid_df = max_bid_df.drop_duplicates()
  max_bid_df = max_bid_df[~max_bid_df.selling_price_usd.isna()]

  total_artist_earning = max_bid_df.groupby('creator').selling_price_usd.sum().to_frame()
  total_artist_earning['artist_id'] = total_artist_earning.index
  total_artist_earning.index = range(len(total_artist_earning))
  #total_artist_earning = pd.merge(total_artist_earning, artist_metadata_df[['artist_id','followers_count','n_followers']])

  t = max_bid_df.groupby('creator').token_id.nunique().to_frame()
  t['artist_id'] = t.index
  t.index = range(len(t))
  t.columns = ['n_art','artist_id']

  total_artist_earning = pd.merge(total_artist_earning, t)
  total_artist_earning = total_artist_earning.sort_values('selling_price_usd', ascending = False)

  owner_df = max_bid_df[['selling_price_usd','token_id','creator','final_d_bidding',
                      'final_dt_bidding','bidder_id']]
  owner_df.columns = ['selling_price_usd','token_id','creator','final_d_bidding','final_dt_bidding','owner']


  total_buyer_spending = owner_df.groupby('owner').selling_price_usd.sum().to_frame()
  total_buyer_spending['buyer'] = total_buyer_spending.index
  total_buyer_spending.index = range(len(total_buyer_spending))

  n_art_owner = owner_df.owner.value_counts().to_frame()
  n_art_owner['own_id'] = n_art_owner.index
  n_art_owner.columns = ['n_art_bought','buyer']
  n_art_owner.index = range(len(n_art_owner))
  total_buyer_spending = pd.merge(total_buyer_spending, n_art_owner, how='left')

  # artist metadata
  artist_metadata_df = pd.read_csv("../data/artist_metadata.csv")
  artist_twitter_df = pd.read_csv("../data/artist_twitter_metadata.csv")
  artist_twitter_df['twitter'] = artist_twitter_df.user_name.apply(lambda x: 'https://twitter.com/'+x)
  tmp = artist_twitter_df[['twitter','followers_count','following_count','verified']]
  tmp.columns = ['twitter','followers_twitter','following_twitter','verified']
  artist_metadata_df = pd.merge(artist_metadata_df, tmp, how='left',on='twitter')

  # invited artist metadata
  invited_by_metadata = pd.read_csv("../data/invited_by_metadata.csv")
  invited_by_twitter = pd.read_csv("../data/invited_by_twitter_metadata.csv")

  invited_by_twitter['twitter'] = invited_by_twitter.user_name.apply(lambda x: 'https://twitter.com/'+x)
  tmp = invited_by_twitter[['twitter','followers_count','following_count','verified']]
  tmp.columns = ['twitter','followers_twitter','following_twitter','verified']

  invited_by_metadata = pd.merge(invited_by_metadata, invited_by_twitter,how='left',on='twitter')

  print("finished loading data...")

  print("stats...")

  print("N art minted:", art_metadata_df.token_id.nunique())
  print("N art listed:", listing_df.token_id.nunique())
  print("N art sold:", art_metadata_df[art_metadata_df.is_sold==True].token_id.nunique())
  print("N creators:", art_metadata_df.creator.nunique())
  print("N buyers:", total_buyer_spending.buyer.nunique())
  print("N bids:", bidding_df.shape[0])

  return art_metadata_df, artist_metadata_df, invited_by_metadata,bidding_df, minting_df, listing_df, max_bid_df, total_artist_earning, total_buyer_spending

