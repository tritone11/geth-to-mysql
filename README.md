# gethtomysql
Basic nodejs to import ethereum blockchain from a geth node to mysql  
Edit sql queries in source.js to adapt them to your mysql db  
You need the genesis block with id 0 in your blocks table  
If you want to restart the import, just delete all blocks from the db and leave the genesis block
  
```
node source.js
```

When synced, it listens for new blocks and add them to the database.  
It supports blocks data, txs, address balances and tx history. 

## PostgreSQL  

psql.py is for postgre, with web3 ipc provider. It is more than 50x faster in the import and it can stay behind a private network with 1s block time.  
This can be used for a high performance explorer, it is currently tested on an explorer made with Django, CitusDB and a blockchain with 1s block time. It is handling more than 500k blocks with an average of 25 transactions

## Notes
It automatically skips empty block, but you can easily disabling this in the syncblock function
