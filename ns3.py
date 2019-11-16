from socket import *
import json
import os,time
from kazoo import client as kz_client
import logging
logging.basicConfig()
my_client = kz_client.KazooClient(hosts='127.0.0.1:2181')

def my_listener(state):
	if state == kz_client.KazooState.CONNECTED:
		print("Client connected !")

my_client.add_listener(my_listener)
my_client.start()
print(("*"*30)+"SERVER"+("*")*30)
children=my_client.get_children("/server/")
children.sort()
#print(children)
msdata=my_client.get("/server/"+children[0])
#print(msdata[0])
mslist=msdata[0].split("=>")
serverName = mslist[0]
serverPort = int(mslist[1])
print(serverName,serverPort)
clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket.connect((serverName,serverPort))
server_id="SZ$JR"
server_id1="JR"
server_port="15000"
server_name="0.0.0.0"
server_port1="$15000"
server_name1="$0.0.0.0"
sbind1=server_id+"=>"+server_name+server_name1+"=>"+server_port+server_port1
clientSocket.send(sbind1)
sbind=server_id+"=>"+server_name+"=>"+server_port
signal = clientSocket.recv(1024)
#print(signal)
clientSocket.close()
sbind1="SZ=AI=>"+server_name+"=>13000"
sentence=[]
skey=""
svalue=""


sdic={}
sdic1={}



@my_client.ChildrenWatch("/server")
def wlis(children):
	global server_port
	msflag=0
	#print(children)
	for i in children:
		if("ms" in i):
			msflag=1
	if(msflag==0): 
		election=my_client.Election("/server/","server")
		print(election.lock.contenders()[0])
		sc=election.lock.contenders()[0].split("=>")
		if(sc[-1]==server_port):
			print("smallest")
			os.system("gnome-terminal -x python nms.py")
			time.sleep(2)
		else:
			time.sleep(7)
		os.system("gnome-terminal -x python ns3.py")
		os._exit(2)
	msflag=0


no_ch=my_client.get_children("/server/")
for i in range(0,len(no_ch)):
	if("server1" in no_ch[i]):
		data=my_client.get("/server/"+no_ch[i])
		print(data[0])
		replist=data[0].split("=>")
		serverName = replist[1]
		serverPort = int(replist[2])
		print(serverName,serverPort)
		clientSocket = socket(AF_INET, SOCK_STREAM)
		try:
			clientSocket.connect((serverName,serverPort))
			sentence=server_name+"=>"+server_port+"=>"+"ret"
			clientSocket.send(sentence)
			rstatus=clientSocket.recv(30000)
			print("GOT DATA FROM REPLICA")
			#print(rstatus)
			data_load=json.loads(rstatus)
			#print(data_load)
			clientSocket.close()
		except:
			pass
		sdic.update(data_load)
		print("sdic",sdic)
		print("RECEIVED DATA:",sdic)
f=0
if(signal=="signal"):
	print("SERVER TO ZOOKEEPER INITIALISATION ")
	my_client.create("/server/server3",sbind,ephemeral=True,sequence=True)
	serverSocket = socket(AF_INET,SOCK_STREAM)
	serverSocket.setsockopt(SOL_SOCKET,SO_REUSEADDR, 1)
	serverSocket.bind((server_name,int(server_port)))
	serverSocket.listen(3)
	print("The server is ready to receive")
	while 1:
		connectionSocket, addr = serverSocket.accept()
		sentence = connectionSocket.recv(1024)
		sentence1=sentence.split("=>")
		skey=sentence1[0]
		svalue=sentence1[1]
		try:
			f=sentence1[2]
		except:
			f=""
		#print(sentence)
		#print(f)
		if(f=="ret"):
			print("SENDING REPLICA CONTENT")
			data_dic=json.dumps(sdic1)
			connectionSocket.send(data_dic)
			connectionSocket.close()
			continue
		k=ord(skey[0])
		if(k>=ord(server_id[0]) and k<=ord(server_id[1])):
			if(svalue==""):
				connectionSocket.send(sdic[skey])
			else:
				sdic[skey]=json.loads(svalue)
				connectionSocket.send("Received")
				no_ch=my_client.get_children("/server/")
				for i in range(0,len(no_ch)):
					if("server1" in no_ch[i]):
						data=my_client.get("/server/"+no_ch[i])
						#print(data[0])
						replist=data[0].split("=>")
						serverName = replist[1]
						serverPort = int(replist[2])
						print("REPLICA SERVER ADDRESS")
						print(serverName,serverPort)
						clientSocket = socket(AF_INET, SOCK_STREAM)
						clientSocket.connect((serverName,serverPort))
						sentence=skey+"=>"+svalue+"=>"+"s"
						print("-"*40)
						print("SENDING SEVER DATA TO REPLICA ")
						print("-"*40)
						clientSocket.send(sentence)
						clientSocket.close()
			print("SERVER DATA")
			print(sdic)
			print("-"*40)
		elif(k>=ord(server_id1[0]) and k<=ord(server_id1[1])):
			children=my_client.get_children("/server/")
			children.sort()
			children=children[1:]
			#print(children)
			if(f!="s"):
				#print("if")
				for i in children:
					if("ms" not in i):
						child_data=my_client.get("/server/"+i)
						cdlist=child_data[0].split("=>")
						cd_id=cdlist[0].split("$")
						#server_list.append(cd_id[0])
						if(k>=ord(cd_id[0][0]) and k<=ord(cd_id[0][1])):
							rf=0
							connectionSocket.send("404")
							connectionSocket.close()
							break
						else:
							rf=1
				if(rf==1):
					print("QUERYING FROM REPLICA DATA")
					if(svalue==""):
						print(skey)
						connectionSocket.send(sdic1[skey])
					else:
						#print("here")
						sdic1[skey]=json.loads(svalue)
						connectionSocket.send("FROM REPLICA")				
			else:
				if(svalue==""):
					#print(skey)
					connectionSocket.send(sdic1[skey])
				else:
					#print("here")
					sdic1[skey]=json.loads(svalue)
					connectionSocket.send("FROM REPLICA")
				print("-"*40)
				print("REPLICA DATA STORED IN SERVER")

				print(sdic1)
				print("-"*40)
		else:
			connectionSocket.send("404")
			connectionSocket.close()
