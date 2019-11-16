from socket import *
from kazoo import client as kz_client
import logging
logging.basicConfig()
my_client = kz_client.KazooClient(hosts='127.0.0.1:2181')

def my_listener(state):
	if state == kz_client.KazooState.CONNECTED:
		print("MASTER CONNECTED TO ZOOKEEPER")

my_client.add_listener(my_listener)
my_client.start()


MserverPort = 12000
serverSocket = socket(AF_INET,SOCK_STREAM)
server_name='0.0.0.0'
print(("*"*30)+"MASTER"+("*")*30)
print(server_name+"=>"+str(MserverPort))
serverSocket.bind((server_name,MserverPort))
serverSocket.listen(1)
print("The server is ready to receive")
'''serverPort1=13000
serverPort2=14000
serverPort3=15000
key_server="AI!"+server_name+"=>"+str(serverPort1)+"|JR!"+server_name+"=>"+str(serverPort2)+"|SZ!"+server_name+"=>"+str(serverPort3)
my_client.set("/server",key_server)'''
print("-"*40)
print("INITIALISING MASTER WITH ZOOKEEPER")
print("-"*40)
my_client.create("/server/ms",server_name+"=>"+str(MserverPort),ephemeral=True,sequence=True)
#print(key_server)
#server_list={"AI":server_name+"=>"+str(serverPort1),"JR":server_name+"=>"+str(serverPort2),"SZ":server_name+"=>"+str(serverPort3)}
server_list={}
rep_list={}
flag=0
fl=0
osc=0 #original server count 
rflag=1
while 1:
    slist=[]
    rlist=[]
    connectionSocket, addr = serverSocket.accept()
    sentence = connectionSocket.recv(1024)
    s=sentence.split("=>")
    #print(s)
    if(len(s)==3):
		sl,rl=s[0].split("$")
		slist.append(sl)
		rlist.append(rl)
		sl,rl=s[1].split("$")
		slist.append(sl)
		rlist.append(rl)
		sl,rl=s[2].split("$")
		slist.append(sl)
		rlist.append(rl)
		#print(slist,type(slist),rlist)
		print("SERVER IS INITIALISING")		
		connectionSocket.send("signal")
		if(flag==0):#for first server 
			skey=slist[0]+"!"+slist[1]+"=>"+slist[2]
			rkey=rlist[0]+"!"+rlist[1]+"=>"+rlist[2]
			fkey=skey #server mapping
			rep=rkey # replica mapping
			flag=1
		else:#for rest of server 
			skey="|"+slist[0]+"!"+slist[1]+"=>"+slist[2]
			rkey="|"+rlist[0]+"!"+rlist[1]+"=>"+rlist[2]
			rep=rep+rkey
			fkey=fkey+skey # append server list
		server_list[slist[0]]=slist[1]+"=>"+slist[2]
		rep_list[rlist[0]]=rlist[1]+"=>"+rlist[2]
		my_client.set("/server",fkey)  # append server list to zookeeper
		osc=osc+1
		print("SERVER CONNECTED")
		print(slist[1]+"=>"+slist[2])
		print("-"*40)
		#print(server_list)
		#print("OSC",osc)
    else:
		children=my_client.get_children("/server/")
		children.sort()
		children=children[1:]
		#print(children)
		for i in children:
			if("ms" not in i): #checking master server 
				child_data=my_client.get("/server/"+i)
				cdlist=child_data[0].split("=>")
				cd_id=cdlist[0].split("$")
				#appending server_list and replica list 
				server_list[cd_id[0]]=cdlist[1]+"=>"+cdlist[2]
				rep_list[cd_id[1]]=cdlist[1]+"=>"+cdlist[2]
				#print(server_list) 
		print("CLIENT CONNECTED ")
		k=ord(s[0][0])
		#print("rl",rep_list)
		#print("sl",server_list)
		for i in server_list.keys():
				if(k>=ord(i[0]) and k<=ord(i[1])): # HASH FUNCTION TO GET CORRECT SERVER
					print("SERVER"+server_list[i])
					connectionSocket.send(server_list[i])
					rflag=0 #NO NEED TO CONNECT TO REPLICA
					break
		if(rflag==1): # CONNECT TO REPLICA
			for i in rep_list.keys():
				print("REPLICA SERVER"+rep_list[i])
				if(k>=ord(i[0]) and k<=ord(i[1])):
					connectionSocket.send(rep_list[i])
		print("CLIENT QUERIED")
		print("-"*40)
		#print("end")
		rflag=1 # ASSUMING SERVER IS DEAD
		server_list={} 
		rl_list={}
				
			
		#r=ord(rlist[0][0])
		'''print(osc)
		if(len(children)==osc):
			print("in")
			print(server_list)
			for i in server_list.keys():
				if(k>=ord(i[0]) and k<=ord(i[1])):
					print(k)
					connectionSocket.send(server_list[i])
					print(server_list[i])
					print("here")
					connectionSocket.close()
					fl=1
		else:
			for j in rep_list.keys():
				print(rep_list)
				if(k>=ord(j[0]) and k<=ord(j[1])):
					connectionSocket.send(rep_list[j])
					print(rep_list[j])
					print("here2")
					connectionSocket.close()
					fl=0'''
		
my_client.stop()
