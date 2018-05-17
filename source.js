var mysql = require('mysql');
var Web3 = require('web3');
var web3 = new Web3();
web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));

var latestSyncedBlock = 0;

function synced() {
  var filter = web3.eth.filter('latest');
  filter.watch(function(error, result) {
    var block = web3.eth.getBlock(result, true);
    var sql = "INSERT INTO block (id,blockhash, miner, timestamp, size, transactions, number, uncle_hash, data, gasused) VALUES ('" + block['number'] + "','" + block.hash + "', '" + block.miner +
      "', '" + block.timestamp + "', '" + block.size + "', '" + block.transactions.length + "', '" + block['number'] + "', '" + block.sha3Uncles + "', '" + block.extraData + "', '" + block.gasUsed +
      "')";
    con.query(sql, function(err, result) {
      if (err) throw err;
      console.log("1 record inserted");
    });
    console.log(block.hash);
    console.log('current block #' + block.number);
  });
}
var con = mysql.createConnection({
  host: "host",
  user: "user",
  password: "password",
  database: "database"
});
con.connect(function(err) {
  if (err) throw err;
  con.query("SELECT * FROM block", function(err, result, fields) {
    if (err) throw err;
    console.log(result[result.length - 1].id);
    latestSyncedBlock = parseInt(result[result.length - 1].id);
    if (latestSyncedBlock < web3.eth.blockNumber) {
      syncBlock();
    }
  });
  console.log("Connected to mysql!");
});

function syncBlock() {
  var blockToSync = latestSyncedBlock + 1;
  var block = web3.eth.getBlock(blockToSync, true);
  var sql = "INSERT INTO block (id,blockhash, miner, timestamp, size, transactions, number, uncle_hash, data, gasused) VALUES ('" + block['number'] + "','" + block.hash + "', '" + block.miner +
    "', '" + block.timestamp + "', '" + block.size + "', '" + block.transactions.length + "', '" + block['number'] + "', '" + block.sha3Uncles + "', '" + block.extraData + "', '" + block.gasUsed +
    "')";
  console.log("syncing block number " + blockToSync);
  syncTransactions(latestSyncedBlock, block, block.timestamp);
  con.query(sql, function(err, result) {
    if (err) throw err;
    console.log("1 block inserted");
    latestSyncedBlock++;
    if (latestSyncedBlock < web3.eth.blockNumber) {
      syncBlock();
    } else {
      synced();
    }
  });
  console.log(block.hash);
  console.log('current block #' + block.number);
}

function syncTransactions(ofblock, block, timestamp) {
  for (var i = 0; i < block.transactions.length; i++) {
    console.log(block.transactions[i]);
    var tx = web3.eth.getTransactionFromBlock(block.hash, i);
    var sql = "INSERT INTO transaction (blocknumber, txid, value, gas, gasprice, nonce, txdata, block_id,sender_id,receiver_id,timestamp) VALUES ('" + ofblock + "','" + tx.hash + "', '" + tx['value'] +
      "', '" + tx.gas + "', '" + tx.gasPrice + "', '" + tx.nonce + "', '" + tx['input'] + "', '" + ofblock + "','" + tx["from"] + "','" + tx["to"] + "','" + timestamp + "')";
    updateAddress(tx["from"], sql);
    updateAddress(tx["to"], false);
  }
}

function updateAddress(address, txsql, timestamp) {
  console.log("checking address");
  console.log(address);
  var sql = "INSERT INTO address (address, balance) VALUES ('" + address + "','" + web3.eth.getBalance(address) + "') ON DUPLICATE KEY UPDATE balance = '" + web3.eth.getBalance(address) + "'";
  con.query(sql, function(err, result) {
    if (err) throw err;
    if (txsql) {
      con.query(txsql, function(err, result) {
        if (err) throw err;
        console.log("1 tx inserted");
        con.query("UPDATE address SET inputcount = (SELECT COUNT(*) FROM transaction WHERE receiver_id = '" + address + "') WHERE address = '" + address + "'", function(err, result) {
          if (err) throw err;
          console.log("tx input count updated");
        });
        con.query("UPDATE address SET outputcount = (SELECT COUNT(*) FROM transaction WHERE sender_id = '" + address + "') WHERE address = '" + address + "'", function(err, result) {
          if (err) throw err;
          console.log("tx output count updated");
        });
      });
    }
  });
}
