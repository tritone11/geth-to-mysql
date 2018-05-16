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
