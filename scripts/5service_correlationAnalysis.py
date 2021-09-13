import paho.mqtt.client as PahoMQTT
import thingspeak
import time
import random
import numpy as np
import requests
import json
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
import matplotlib.pyplot as plt
import os
import cherrypy
from collections import Counter

class corrAnalysis(object):
    
    exposed=True

    def GET(self,*uri,**params):
        
        # Salviamo l'uri (in uri) e i parametri (in lista) passati con la get request
        uri = uri[0]
        
        if params!={}:
            valuesList = [params[key] for key in params.keys()]
            lista = []
            
            for el in valuesList:
                lista.append(el)
        
        # Splittiamo le possibili get requests tramite uri
        if uri == "CheckDiscounts":
            
            # Richiamiamo la funzione che calcola la correlation analysis
            corrAnalysis = self.compute_correlation_analysis()
            
            # Se riceviamo come output un dizionario contenente la chiave "error",
            # significa che qualcosa è andato storto
            if "error" in corrAnalysis.keys():
                # In tal caso, restituiremo un json contenente il messaggio di errore
                # che spiega nello specifico cosa non ha funzionato
                response = {"error" : corrAnalysis["error"]}
                return json.dumps(response, indent=4)
            
            # Non ci dovesse essere alcuna chiave "error", procediamo ad analizzare 
            # gli sconti ricavati dalla nostra funzione
            discounted_products = corrAnalysis["discounted_products"]
            corr_products = corrAnalysis["corr_products"]
            
            # Inizializziamo un template della risposta che restituiremo
            response = {"products" : []}
            
            # Se il primo parametro della get request è Single Products, restituiamo
            # i singoli prodotti scontati
            if lista[0] == 'SingleProducts':
                # Se la lista di prodotti scontati è al momento vuota, restituiamo
                # un messaggio che comunichi l'assenza di sconti al momento
                if discounted_products == []:
                    response["error"] = "There are not discounted products at the moment."
                
                # In caso contrario restituiamo la lista dei prodotti scontati
                response["products"] = discounted_products
                
            # Altrimenti restituiamo gli sconti di "coppia" antecedenti-conseguenti
            elif lista[0] == 'CoupledProducts':
                # Se la lista di prodotti scontati è al momento vuota, restituiamo
                # un messaggio che comunichi l'assenza di sconti al momento
                if corr_products == []:
                    response["error"] = "There are not discounted products at the moment."
                
                # In caso contrario restituiamo la lista dei prodotti scontati
                response["products"] = corr_products
                
            return json.dumps(response, indent=4)
        
        # Infine, potremmo anche voler calcolare il costo totale delle singole transaction 
        elif uri == "getTotal":
            
            response = []
            
            for el in self.transactions:
                response.append({"day":el["timeStamp"].split("/")[0],"total":el["total"]})
            return json.dumps(response, indent=4)
    
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
        self.transactions = file.read()
        file.close()
        self.transactions = json.loads(self.transactions)
            
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
        
        # Appendiamo la nuova transaction nella lista già in nostro possesso, caricata
        # nell'init
        self.transactions.append({"transaction" : payload["transaction"], "timeStamp" : payload["timeStamp"], "total" : payload["total"]})
        
        # Aggiorniamo il nostro database di transactions in modo da tenerlo sempre aggiornato
        json.dump(self.transactions,open(str(self.transactionsDB),"w"),indent=4)
        
    def compute_correlation_analysis(self):
        # Il nostro obbiettivo è quello di andare a calcolare la correlazione tra
        # i diversi prodotti delle singole transactions, in modo da ottenere 
        # delle statistiche che ci permettano di ideare degli sconti più adatti 
        # possibili alle abitudini dei clienti. Questi ultimi, invogliati dagli
        # sconti, saranno più propensi ad acquistare un numero maggiore di prodotti,
        # aumentando le entrate del supermarket.
        # Per fare ciò, sfrutteremo le "Association Rules", in particolari l'apriori
        # algorithm, che funziona su liste di Vero/Falso o 1/0
        
        # Utilizzando il TransactionEncoder, vogliamo convertire le transazioni
        # ad una One-Hot Encoded Boolean list. i prodotti acquistati dai clienti saranno
        # rappresentati da un 1, quelli non acquistati da uno 0
        a = TransactionEncoder()
        
        # Inizializziamo delle liste vuote che andremo a riempire nel corso di questa
        # analisi
        nuovaLista = []
        discounted_products = []
        corr_products = []
        response = {"discounted_products" : [], "corr_products" : []}
        
        # Calcoleremo le statistiche solo avendo almeno 10 transazioni nel nostro 
        # database; avendone meno, il rischio sarebbe quello di andare ad ottenere
        # delle statistiche troppo poco significative, che porterebbero a degli sconti
        # poco sensati
        if len(self.transactions)>10:
            
            # Iteriamo sulle transazioni a nostra disposizione e appendiamole 
            # in una nuova lista
            for diction in self.transactions:
                nuovaLista.append(diction["transaction"])
            
            # Effettuiamo la trasformazione vera e propria della lista con il 
            # One Hot Encoding e salviamola in un dataframe pandas
            a_data = a.fit(nuovaLista).transform(nuovaLista)
            df = pd.DataFrame(a_data,columns=a.columns_)
            df = df.replace(False,0)

            #Adesso creiamo il modello a priori vero e proprio. Settiamo un threshold
            # per il valore del support in modo da ottenere solo valori superiori a tale
            # limite.
            # SUPPORT: Probabilità che un evento accada. In questo caso, è la probabilità 
            # che un articolo venga acquistato
            df = apriori(df, min_support = 0.2, use_colnames = True, verbose = 1)
            
            # Ordiniamo il dataframe in base al valore di support
            dfSorted = df.sort_values(by=['support'])
            
            #Adesso possiamo calcolare le association rules; la metrica utilizzata è quella
            # della confidence, con un threshold minimo posto a 0.6
            # CONFIDENCE: Misura di una probabilità condizionata. frq(X,Y)/frq(X)
            # In questo caso abbiamo scelto un valore di confidence minimo del 60%,
            # il che significa che quando un prodotto X è acquistato, possiamo 
            # assumere che nel 60% dei casi o più verrà acquistato anche il 
            # prodotto Y
            df_ar = association_rules(df, metric = "confidence", min_threshold = 0.6)
            
            # Inizializziamo la lista dei prodotti singoli da scontare in base
            # al valore del loro support
            productsList=[]
            
            # Iteriamo sul dataframe ordinato per support
            for index,row in dfSorted.iterrows():
                # Se il support è compreso tra il 10 e il 30% ed è un singolo prodotto,
                # inseriamolo nella lista dei prodotti da scontare
                if row["support"] >= 0.1 and row["support"] <= 0.3 and len(list(row["itemsets"]))==1:
                    productsList.append(str(list(row["itemsets"])[0]))
            
            # Facciamo una richiesta al catalog per ottenere l'URL del TS Adaptor
            payload = {"ID" : "TSAdaptor"}
            r = requests.get(urlcatalog + "/getEndPoints", params=payload)
            dictionary = r.json()
            # Se non è online al momento, printiamolo e restituiamolo come risposta 
            # a chiunque richieda i suoi servizi
            if not dictionary["exist"]:
                response["error"] = "The TSAdaptor service is not currently available, retry later!"
                print("The TSAdaptor service is not currently available, retry later!")
                return response
            
            # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo 
            # e chiediamo i prezzi dei prodotti presenti nella lista dei prodotti
            # da scontare, dandogli tale lista come parametro
            urlTSA = dictionary['url']+'/getPrices'
            payload = {"discountedList" : productsList}
            r2 = requests.get(urlTSA,params=payload)
            response2 = r2.json()
            
            # Iteriamo sul dataframe dei prodotti di cui abbiamo calcolato il support
            for index,row in dfSorted.iterrows():
                # Se il support è compreso tra il 10 e il 30%
                if row["support"] >= 0.1 and row["support"] <= 0.3 and len(list(row["itemsets"]))==1:
                    # Individuiamo l'indice di tale prodotto nella lista contenente
                    # i prezzi base dei singoli prodotti e salviamo il prezzo in 
                    # una variabile
                    idx = response2["products"].index(str(list(row["itemsets"])[0]))
                    basePrice = float(response2["prices"][idx])
                    
                    # Splittiamo i vari casi in base al valore del support. Più
                    # il support è basso, meno è probabile che l'oggetto venga acquistato,
                    # di conseguenza proporremo uno sconto più aggressivo in modo
                    # da invogliare il cliente all'acquisto
                    if row["support"] >= 0.1 and row["support"] <= 0.2 and len(list(row["itemsets"]))==1:
                        # 30% di sconto
                        discounted_products.append(str(list(row["itemsets"])[0])+"/"+str(format(basePrice,".2f"))+"/"+str(format(round(basePrice-basePrice*0.3,2),".2f")))
                    elif row["support"] > 0.2 and row["support"] <= 0.25 and len(list(row["itemsets"]))==1:
                        # 20% di sconto
                        discounted_products.append(str(list(row["itemsets"])[0])+"/"+str(format(basePrice,".2f"))+"/"+str(format(round(basePrice-basePrice*0.2,2),".2f")))
                    elif row["support"] > 0.25 and row["support"] <= 0.3 and len(list(row["itemsets"]))==1:
                        # 15% di sconto
                        discounted_products.append(str(list(row["itemsets"])[0])+"/"+str(format(basePrice,".2f"))+"/"+str(format(round(basePrice-basePrice*0.15,2),".2f")))
            
            # Stavolta iteriamo sul dataframe delle association rules
            for index,row in df_ar.iterrows():
                # Se la confidence è compresa tra il 60 e l'85%, vogliamo aumentare
                # la probabilità che un cliente acquisti la merce Y una volta
                # che acquista l'oggetto X, di conseguenza scontiamo l'oggetto Y
                # se vengono acquistati anche gli oggetti X.
                if row["confidence"] >= 0.6 and row["confidence"] <= 0.85:
                    corr_prod = {"antecedents" : list(row["antecedents"]), "consequents" : list(row["consequents"])}
                    corr_products.append(corr_prod)
            
            # Nella risposta, inseriamo le due liste di prodotti singoli/coppie
            # scontati e restituiamola 
            response["discounted_products"] = discounted_products
            response["corr_products"] = corr_products

        return response

if __name__ == "__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # Inizializziamo il nostro oggetto correlation_computer e startiamolo
    pubID = 'CorrelationAnalysisDiscounts'
    laneTopic = dictionary["topic_reader_lane"]
    correlation_computer=corrAnalysis("transactionsDB.json", laneTopic, pubID, dictionary['broker'], dictionary['port'])
    correlation_computer.start()
     
    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tool.session.on':True        
            }    
        }   
    cherrypy.config.update({'server.socket_port':8060}) 
    cherrypy.tree.mount(corrAnalysis("transactionsDB.json", laneTopic, pubID, dictionary['broker'], dictionary['port']),'/',conf)
    cherrypy.engine.start()
    
    
    # Ogni 5 minuti, inviamo una post request al catalog per comunicare che 
    # il servizio è online, con il relativo URL al quale è raggiungibile
    while True:
        payload = {"ID" : "CheckDiscounts", "url" : "http://127.0.0.1:8060"}
        payload = json.dumps(payload,indent=4)
        requests.post(urlcatalog + "/updateOnlineService",data=payload)
        time.sleep(300)