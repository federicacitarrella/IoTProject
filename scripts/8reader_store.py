import paho.mqtt.client as PahoMQTT
import thingspeak
import time
import random
import numpy as np
import requests
import json

class RFID_reader(object):

    def __init__(self, baseTopic, updateTopic, Topics, channels, clientID, broker, port):
        self.clientID= clientID 
        self.broker=broker
        self.port=port
        
        self.baseTopic = baseTopic
        self.updateTopic = updateTopic
        self.Topics = Topics
        self.laneTopic = Topics[0]
        self.restockTopic = Topics[1]
        
        self.channels = channels
        
        self._paho_mqtt = PahoMQTT.Client(self.clientID,True)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived
        
        # Inizializziamo una lista vuota che riempiremo volta per volta
        self.quantityProducts = []

    def start(self):
        # Quando startiamo l'RFID_reader, lo connettiamo al broker e lo
        # iscriviamo subito al topic di lane topic, che pubblica tutte le transazioni
        # volta per volta
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()
        for topic in self.Topics:
            self._paho_mqtt.subscribe(topic)
        
        # La quantità iniziale di ogni prodotto viene fissata a 20
        quantityInit = 20
        
        # Creiamo una lista vuota event, che verrà utilizzata come template
        event = []
    
        # Su thingspeak abbiamo 4 canali, uno per ogni reparto (Food, Beverage,
        # Personal Care ed Electronics). Iteriamo su ognuno di essi
        for i,channel in enumerate(self.channels):
            # Recuperiamo il numero di fields presenti in ogni canale e appendiamo
            # alla lista vuota creata nell'init un dizionario per ogni prodotto,
            # avente al suo interno l'id del canale e una lista della lunghezza
            # corrispondente al numero di prodotti contenuti nel canale, ogni valore 
            # corrisponde alla quantità del corrispondente prodotto
            nFields = len(channel["fields"])
            self.quantityProducts.append({"id":channel["id"],"quantities":[quantityInit]*nFields,"names":[name for name in channel["fields"]]})
            
            # Appendiamo ad event un dizionario contenente il channel ID, il nome 
            # dei singoli prodotti contenuti nel canale attuale, le loro quantità 
            # e un timestamp che ci dia un informazione temporale su quando
            # è stato effettuato l'ultimo aggiornamento
            event.append({'n':channel['id'],'productsNames':channel['fields'],'values':self.quantityProducts[i]['quantities'],'t':time.time()})
         
        # Creiamo un messaggio di thingspeak
        message_thingspeak = {
            "bn":self.baseTopic,
            "e":event
            }
        
        # E un messaggio di update
        message_update = {"Update":1}

        # Che verranno pubblicati nei rispettivi topic
        self.publish(self.baseTopic,json.dumps(message_thingspeak,indent=4))
        self.publish(self.updateTopic,json.dumps(message_update,indent=4))   

    def stop(self):
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))
        
    def myOnMessageReceived (self, paho_mqtt , userdata, msg):
        self.notify (msg.topic, msg.payload)
        
    def notify(self,topic,payload):
        # Quando la smart lane pubblica un messaggio, lo salviamo nel payload
        payload = json.loads(payload)
        
        if topic == self.laneTopic:
            # Ricaviamo la transaction appena segnalata dalla smart lane
            transaction = payload["transaction"]
            
            # Inizializziamo una lista event vuota
            event = []
            
            # Iteriamo su tutti i canali
            for i,channel in enumerate(self.channels):
                # E su tutti i fields. Ogni fields corrisponde ad un prodotto
                for j,field in enumerate(channel['fields']):
                    # Se tale field è contenuto all'interno della transazione
                    # pubblicata dalla smart lane, significa che tale prodotto è stato
                    # acquistato, di conseguenza dovremo diminuire la quantità del 
                    # prodotto su thingspeak 
                    if field in transaction:
                        # Contiamo quante volte è presente tale prodotto nella transaction,
                        # dato che potrebbe esser stato acquistato in più confezioni
                        n_bought = transaction.count(field)
                        
                        # Diminuiamo la quantità del relativo prodotto di n_bought
                        self.quantityProducts[i]['quantities'][j] -= n_bought
                
                # Appendiamo ad event il nuovo messaggio con le nuove quantità
                event.append({'n':channel['id'],'productsNames':channel['fields'],'values':self.quantityProducts[i]['quantities'],'t':time.time()})
            
            # Creiamo il messaggio di thingspeak
            message_thingspeak = {
                "bn":self.baseTopic,
                "e":event
                }
            
            # E un messaggio di update
            message_update = {"Update":1}

            # Che verranno pubblicati nei rispettivi topic, andando ad aggiornare thingspeak
            self.publish(self.baseTopic,json.dumps(message_thingspeak,indent=4))
            self.publish(self.updateTopic,json.dumps(message_update,indent=4))
        
        elif topic == self.restockTopic:
            restockQuantity = 20
        
            for prod in payload["restockProducts"]:
                for idx1,channel in enumerate(self.quantityProducts):
                    for idx2,name in enumerate(channel["names"]):
                        if name == prod:
                            self.quantityProducts[idx1]["quantities"][idx2] = restockQuantity

            self.publish(self.baseTopic,json.dumps(payload,indent=4))

    def publish(self, topic, message):
        self._paho_mqtt.publish(topic, message, 2)

if __name__ == "__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # Inizializziamo un payload in cui inseriamo l'ID del microservice che
    # vogliamo raggiungere
    payload = {"ID" : "TSAdaptor"}
        
    # Tramite get request contattiamo il catalog, chiedendo se il microservice
    # desiderato è online e, in tal caso, ricevendo il suo URL
    r2 = requests.get(urlcatalog + "/getEndPoints", params=payload)
    dictionary2 = r2.json()
        
    # Se non è online al momento, printiamolo e restituiamolo come risposta 
    # a chiunque richieda i suoi servizi
    if not dictionary2["exist"]:
        print("The TSAdaptor service is not currently available, retry later!")
        exit()
                
    # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
    # restituiamo i dati desiderati
    urlTSA = dictionary2['url']
    
    # Tramite get request, otteniamo dal thingspeak adaptor i nostri canali,
    # che daremo in input al myReader_store
    channels = requests.get(urlTSA+'/getChannels')
    channels = channels.json()
    
    # Selezioniamo i topic di nostro interesse
    laneTopic = dictionary["topic_reader_lane"]    
    baseTopic = dictionary["topic_reader_store"]
    updateTopic = dictionary["topic_thingspeak_updated"]
    restockTopic = dictionary["topic_reader_store_restock"]
    
    # Inizializziamo il nostro oggetto RFID_reader e startiamolo
    pubID = "MarketReaderStore"
    myReader_store=RFID_reader(baseTopic, updateTopic, [laneTopic,restockTopic], channels, pubID, dictionary['broker'], dictionary['port'])
    myReader_store.start()


    while True:
        time.sleep(5)
   