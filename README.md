# Crypto Art Data

`read_data.py` file contains the script to load and process the NFT data. 
`query_FND_graph.ipynb` file contains the script to extract the latest data version from Foundation. 

Usage :
```
from read_data import load_data()
art_metadata_df, artist_metadata_df, invited_by_metadata,bidding_df, minting_df, listing_df, max_bid_df,total_artist_earning, total_buyer_spending = load_data()
```

Stats about the data:

```
N art minted: 50723
N art listed: 48059
N art re-listed: 1928
N art sold: 15279
N art resold: 138
N creators: 15366
N buyers: 5531
N bids: 37013
```
