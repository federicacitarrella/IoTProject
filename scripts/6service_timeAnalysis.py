import paho.mqtt.client as PahoMQTT
import thingspeak
import time
import random
import numpy as np
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
import os
import cherrypy
from collections import Counter

class timeAnalysis(object):
    
    exposed=True

    def GET(self,*uri,**params):
        
        # Salviamo l'uri (in uri) e i parametri (in lista) passati con la get request
        uri = uri[0]
        
        if params!={}:
            valuesList = [params[key] for key in params.keys()]
            lista = []
            
            for el in valuesList:
                lista.append(el)
        
        # Nel caso in cui l'uri sia TimeDistribution, restituiamo il grafico
        # delle transazioni distribuite nel tempo, siano esse di un singolo giorno
        # o dell'intera settimana (a seconda del parametro passato nella get request)
        if uri == "TimeDistribution":
            outList = self.compute_time_analysis(lista[0])
            return json.dumps(outList, indent=4)
        
    
    def __init__(self, transactionsDB, laneTopic, clientID, broker, port):
        self.clientID= clientID 
        self.broker=broker
        self.port=port
        
        self.laneTopic = laneTopic
        
        self._paho_mqtt = PahoMQTT.Client(self.clientID,True)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived
        
        # Il nostro script si basa su un "Database" di transactions salvate in locale
        # nello stesso ambiente in cui il microservice viene runnato. Non sapendo
        # gestire veri database, abbiamo creato un file json "transactionDB" che 
        # potesse fungere come tale
        self.transactionsDB = transactionsDB
        
        # Vogliamo avere una lista dei file contenuti nella current directory
        fileList = os.listdir(os.getcwd())

        # Se in tale lista non esiste il nostro DB di transactions, creiamolo vuoto
        # in modo che possa esser riempito volta per volta, ogniqualvolta
        # la smart lane pubblichi via MQTT una nuova transaction. In tal caso, 
        # essendo questo script iscritto allo specifico topic della smart lane, 
        # provvederà a salvare ogni nuova transaction in tale database, mantenendolo 
        # sempre aggiornato
        if not self.transactionsDB in fileList:
            file = open(self.transactionsDB,"w")
            file.write("[]")
            file.close()
            
        # Nel caso in cui il database fosse già presente nella current directory,
        # apriamolo e leggiamolo in modo da recuperare tutte le transactions passate.
        # In questo modo, le statistiche saranno sempre calcolate su tutti i dati 
        # in nostro possesso
        file = open(self.transactionsDB,"r")
        self.transactions_time = file.read()
        file.close()
        self.transactions_time = json.loads(self.transactions_time)
            
    def start(self):
        # Quando startiamo il correlation computer, lo connettiamo al broker e lo
        # iscriviamo subito al topic della smart lane, che pubblica tutte le transazioni
        # volta per volta
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()
        self._paho_mqtt.subscribe(self.laneTopic)
        
    def stop(self):
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))
        
    def myOnMessageReceived (self, paho_mqtt , userdata, msg):
        self.notify (msg.topic, msg.payload)
        
    def notify(self,topic,payload):
        # Ogni volta che riceviamo una nuova transaction dalla smart lane, la salviamo
        # nel payload
        payload = json.loads(payload)
        
        # Appendiamo il timeStamp della nuova transaction nella lista già in nostro possesso, caricata
        # nell'init
        self.transactions_time.append(payload["timeStamp"])
        
        # Aggiorniamo il nostro database di transactions in modo da tenerlo sempre aggiornato
        json.dump(self.transactions_time,open(str(self.transactionsDB),"w"),indent=4)
        
    def compute_time_analysis(self, cmd):
        # Inizializziamo una lista di output vuota
        outList = []
        
        # In base al cmd di input restituiamo gli orari di un singolo giorno o
        # dell'intera settimana
        if cmd=='week':
            for timeTrans in self.transactions_time:
                outList.append(timeTrans.split('/')[0])
        else:
            for timeTrans in self.transactions_time:
                if timeTrans.split('/')[0]==cmd:
                    outList.append(timeTrans.split('/')[1].split(':')[0])
        return outList


if __name__ == "__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # Inizializziamo il nostro oggetto correlation_computer e startiamolo
    pubID = 'TimeAnalysis'
    laneTopic = dictionary["topic_reader_lane"]
    time_computer=timeAnalysis("transactionsDBtime.json", laneTopic, pubID, dictionary['broker'], dictionary['port'])
    time_computer.start()
     
    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tool.session.on':True        
            }    
        }   
    cherrypy.config.update({'server.socket_port':8070}) 
    cherrypy.tree.mount(timeAnalysis("transactionsDBtime.json", laneTopic, pubID, dictionary['broker'], dictionary['port']),'/',conf)
    cherrypy.engine.start()
    
    
    # Ogni 5 minuti, inviamo una post request al catalog per comunicare che 
    # il servizio è online, con il relativo URL al quale è raggiungibile
    while True:
        payload = {"ID" : "TimeAnalysis", "url" : "http://127.0.0.1:8070"}
        payload = json.dumps(payload,indent=4)
        requests.post(urlcatalog + "/updateOnlineService",data=payload)
        time.sleep(300)