#!/usr/bin/env python3
from web3 import Web3
import psycopg2
import asyncio
from threading import Thread
import time
import uuid , binascii
import codecs
import pprint

block=None
blockToSync=0
batchtran=100
latestSyncedBlock=0
initialTran=0
latestSynceTran=0
import sys


def synctran(start,end):
    start=start+1
    sql="update address r set inputcount=inputcount + c.receivesum,outputcount=outputcount + c.sendsum from ( select distinct COALESCE(a.id,b.id) as addr,COALESCE(b.sendsum,0) as sendsum,COALESCE(a.receivesum,0) as receivesum  from ( select receiver as id,count(transaction.id) as receivesum from transaction  join address  on address.address=transaction.receiver  where address.balance<>'0' and transaction.id between " + str(start) +  " and " +  str(end) + " group by receiver ) as  a full  join  ( select sender as id,count(transaction.id) as sendsum from transaction  join address  on address.address=transaction.sender  where address.balance<>'0' and  transaction.id between " +  str(start)  + " and " + str(end) + " group by sender ) as  b on a.id=b.id ) as c WHERE r.address=c.addr " 
    try:
       cur2.execute(sql)
    except psycopg2.Error as e:
        print(str(e))

def execquery(sql):
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        print(str(e))
        


def addresscounter():
    cur2 = conn.cursor()
    sql = "select address from address where balance <>'0'"
    try:
        cur2.execute(sql)
    except psycopg2.Error as e:
        print(str(e))
    row = cur2.fetchone()
    while row:
        sql="UPDATE address SET inputcount = (SELECT COUNT(*) FROM transaction WHERE receiver = '" + row[0] + "') WHERE address = '" +  row[0]  + "' "
        execquery(str(sql))
        sql="UPDATE address SET outputcount = (SELECT COUNT(*) FROM transaction WHERE sender = '" +  row[0] + "') WHERE address = '" +   row[0] + "' "
        execquery(str(sql))
        row = cur2.fetchone()
    

def updateAddress(address, fromtr):
    print("checking address")
    print(address)

    
    if(fromtr):
        sql = "INSERT INTO address (address,inputcount,outputcount) VALUES ('" + str(address) + "',0,1) ON CONFLICT (address) DO UPDATE set outputcount = address.outputcount + 1"
    else:
        sql = "INSERT INTO address (address,inputcount,outputcount) VALUES ('" + str(address) + "',1,0) ON CONFLICT (address) DO UPDATE set inputcount=address.inputcount+1"
    execquery(str(sql))


def syncTransactions(ofblock, block, timestamp):
    global latestSyncedBlock
    global initialTran
    global latestSyncedTran
    for i in range(0,  len(block.transactions) ):
        tx=block.transactions[i]
        sql = "INSERT INTO transaction (txid, value, gas, gasprice, nonce, txdata, block,sender,receiver,timestamp) VALUES ('" 
        sql=sql + web3.toHex(tx.hash) + "', '" + str(tx.value) + "', '" + str(tx.gas) + "', '" + str(tx.gasPrice) + "', '" + str(tx.nonce) + "', '" + str(tx.input)
        sql=sql+ "', '" + str(ofblock) + "','" +  str(tx["from"]) + "','" + str(tx.to) + "',to_timestamp(" + str(timestamp) + ") at time zone 'UTC' )  ON CONFLICT (txid) DO NOTHING "
        execquery(str(sql))
        updateAddress(str(tx["from"]), True)
        updateAddress(str(tx.to), False)
    sql = "END TRANSACTION;"
    execquery(str(sql))
        
	

def syncBlock(block):
    global latestSyncedBlock
    global initialTran
    global latestSyncedTran
    if(not block):
        blockToSync = latestSyncedBlock + 1
        block = web3.eth.getBlock(blockToSync, True)
    else:
        blockToSync = block
        print(str(block) + "||||||||||")
        block = web3.eth.getBlock(blockToSync, True)
    if block != None:
        sql = "BEGIN TRANSACTION;INSERT INTO block (id,blockhash, miner, timestamp, size, transactions, number, uncle_hash, data, gasused) VALUES ('" + str(block.number) + "','" +   web3.toHex(block.hash)  + "', '" + str(block.miner) + "', to_timestamp(" + str(block.timestamp) + ") at time zone 'UTC', '" + str(block.size) + "', '" +  str(len(block.transactions))  + "', '" + str(block.number) + "', '', '" + web3.toHex(block.extraData) + "', '" + str(block.gasUsed) + "') ON CONFLICT (id) do UPDATE set blockhash='"+ str(block.number) +"', miner='" + str(block.miner) + "', timestamp=to_timestamp(" + str(block.timestamp) + ") at time zone 'UTC',size='"+ str(block.size) + "',transactions='" +str(len(block.transactions)) + "',number='"+  str(block.number) + "',uncle_hash='',data='" + web3.toHex(block.extraData) + "',gasused='" + str(block.gasUsed) +"' "
        print("syncing block number " + str(blockToSync))
        print ("got n: " + str(len(block.transactions)) + " transactions")
        if len(block.transactions) > 0:
            execquery(str(sql))
            syncTransactions(blockToSync, block, block.timestamp)
            print("1 block inserted")
            print(str(block.hash))
            print('current block #' + str(block.number))
        else:
            print("1 block inserted")
            print('jumping empty block #' + str(block.number))





def log_loop(event_filter, poll_interval):
    while True:
        for event in event_filter.get_new_entries():
            block = web3.eth.getBlock(event)
            print ("new block:" ,block.number)
            syncBlock(block.number)




def synced():
    global initialTran
    global latestSyncedTran
    print("SYNCED")
    block_filter = web3.eth.filter('latest')
    log_loop(block_filter, 5)

def main():
    global block
    global latestSyncedBlock
    global initialTran
    global latestSyncedTran
    global latestdBlock
    cur.execute("SELECT max(id) as total FROM block  ")
    result = cur.fetchone()
    print (result[0])
    print('latestSyncedBlock block:',result[0])
    latestSyncedBlock=int(result[0])
    latestdBlock=web3.eth.blockNumber
    while (latestSyncedBlock <= latestdBlock):
        print ('Latest block:',latestdBlock)
        if ( latestSyncedBlock == latestdBlock):
            latestdBlock=web3.eth.blockNumber
        latestSyncedBlock=latestSyncedBlock+1
        syncBlock(latestSyncedBlock)
    synced()

	
if __name__ == '__main__':
    sys.setrecursionlimit(100000)
    conn = psycopg2.connect(database='database', user='user', host='127.0.0.1', port=5432, sslmode='disable')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    web3 = Web3(Web3.IPCProvider("/home/user/ethereum/datadir/geth.ipc"))
       
    main()
