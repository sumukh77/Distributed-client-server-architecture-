#-*- coding: utf-8 -*-
"""
Created on Sun Oct 28 10:55:28 2018

@author: sumuk
"""
import json
from socket import *
from kazoo import client as kz_client
import logging
logging.basicConfig()
my_client = kz_client.KazooClient(hosts='127.0.0.1:2181')

def my_listener(state):
	if state == kz_client.KazooState.CONNECTED:
		print("Client connected !")

my_client.add_listener(my_listener)
my_client.start()

children=my_client.get_children("/server/")
children.sort()
#print(children)
#CONNECTION TO MASTER 
if("ms" in children[0]):
	msdata=my_client.get("/server/"+children[0])
else:
	exit(1)
#print(msdata[0])
#
mslist=msdata[0].split("=>") #GETTING MASTER SERVER NAME AND PORT
serverName = mslist[0]
serverPort = int(mslist[1])
print("MASTER CONNECTION ESTABLISHED")
print(msdata[0])
print("*"*40)
#print(serverName,serverPort)
clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket.connect((serverName,serverPort))
key_val = raw_input("ENTER DATA ").upper()
print("-"*40)
ilist=key_val.split(' ')
try:
    v=json.dumps(ilist[1])
except:
    v=""

key=ilist[0]+"=>"+v    
clientSocket.send(key)
kflag=1 #to check if data is already given
ser = clientSocket.recv(1024)
clientSocket.close()
print("SERVER CONNECTION ESTABLISHED") #connecting to server for the first time 
print(ser)
print("-"*40)
sn,sp=ser.split("=>")
#print(sp)
mflag=0 #to check if data is already given and send it to master 
f=0 #if server is dead connect to master 
rflag=0 #to use replica server 
clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket.connect((sn,int(sp)))
while 1:
	
	'''try:
		clientSocket.connect((sn,int(sp)))
		f=1
		print("hoooo")
	except:
		pass'''
	
	if(kflag==0): 
		clientSocket = socket(AF_INET, SOCK_STREAM)
		key_val = raw_input("ENTER DATA ").upper()
		print("-"*40)
		try:
			clientSocket.connect((sn,int(sp)))
		except:
			f=0
			print("SERVER DEAD ")
			print("-"*40)
			if(f==0):
				print("CONNECTING TO MASTER")
				print("-"*40)
				clientSocket.connect((serverName,serverPort))
				rflag=1#CONNECT TO REPLICA
		ilist=key_val.split(' ')
		try:
			v=json.dumps(ilist[1])
		except:
			v=""
		key=ilist[0]+"=>"+v
	clientSocket.send(key)
	print("DATA SENT")
	kflag=0 # AGAIN TO GET DATA FOR NEXT TIME 
	server_status=clientSocket.recv(1024)
	print("SERVER STATS:-"+server_status.upper())
	print("-"*40)
	if(rflag==1):#WHEN REPLICA NEEDS TO BE ACCESSED
		clientSocket.close()
		sn,sp=server_status.split("=>")
		clientSocket = socket(AF_INET, SOCK_STREAM)
		clientSocket.connect((sn,int(sp)))
		clientSocket.send(key)
		status=clientSocket.recv(1024)
		rflag=0
		clientSocket.close()
		print("SERVER STATUS:-"+status.upper())
		print("-"*40)
	#print(mflag)
	
	if(mflag==1):#CONNECT TO CORRECT SERVER
		sn,sp=server_status.split("=>")
		mflag=0
		kflag=1
		clientSocket = socket(AF_INET, SOCK_STREAM)
		clientSocket.connect((sn,int(sp))) # CONNECTING TO CORRECT TO SERVER 
	if(server_status=="404"):#INCASE OF ERROR CONNECT TO MASTER 
		clientSocket.close() # CLOSE THE CONNECTION IN CASE OF WRONG KEY
		kflag=1
		f=1
		children=my_client.get_children("/server/")
		children.sort()
		#print(children)
		if("ms" in children[0]):
			msdata=my_client.get("/server/"+children[0])
		else:
			exit(1)
		#print(msdata[0])
		print("CONNECTED TO MASTER ")
		print("-"*40)
		mslist=msdata[0].split("=>")
		sn = mslist[0]
		sp = int(mslist[1])
		mflag=1
		clientSocket = socket(AF_INET, SOCK_STREAM)
		clientSocket.connect((sn,int(sp))) #CONNECTING TO MASTER 
		
	

