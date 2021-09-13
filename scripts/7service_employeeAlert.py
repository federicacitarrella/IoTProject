import cherrypy
import time
import json
import paho.mqtt.client as PahoMQTT
import requests

class EmployeeAlert(object):
    exposed=True

    def __init__(self, clientID, topic, topicBot, topicTS, topicRestock, broker, port):
        self.clientID= clientID 
        self.broker=broker
        self.port=port
        self.topic = topic
        self.topicRestock = topicRestock
        self.topicBot = topicBot
        self.topicTS = topicTS
        
        self._paho_mqtt = PahoMQTT.Client(self.clientID,True)
        
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

    def myOnMessageReceived(self, paho_mqtt , userdata, msg):
        self.notify(msg.topic, msg.payload)

    def start(self):
        # Quando startiamo l'EmployeeAlert, lo connettiamo al broker e lo
        # iscriviamo subito al topic di thingspeak updated. Ogni volta che 
        # thingspeak viene aggiornato, pubblica un messaggio su questo topic
        # avvisando così l'alert del cambiamento di quantità dei prodotti sugli 
        # scaffali
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()
        self._paho_mqtt.subscribe(self.topic,2)


    def stop(self):
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))

    def notify(self,topic,payload):
        # Salviamo il messaggio nella variabile payload
        payload = json.loads(payload)
        
        # Se la chiave Update ha come valore associato 1, significa che thingspeak
        # è stato aggiornato, di conseguenza i prodotti son diminuiti.
        # Vogliamo verificare quanti prodotti abbiamo per ogni categoria, in modo
        # da rifornire gli scaffali quando si scende al di sotto di un certo quantitativo
        if int(payload["Update"])==1:
            # Inizializziamo un payload in cui inseriamo l'ID del microservice che
            # vogliamo raggiungere
            payload = {"ID" : "TSAdaptor"}
             
            # Tramite get request contattiamo il catalog, chiedendo se il microservice
            # desiderato è online e, in tal caso, ricevendo il suo URL
            r = requests.get(urlcatalog + "/getEndPoints", params=payload)
            dictionary = r.json()
            
            # Se non è online al momento, printiamolo e restituiamolo come risposta 
            # a chiunque richieda i suoi servizi
            if not dictionary["exist"]:
                response = {"error" : "The TSAdaptor service is not currently available, retry later!"}
                print("The TSAdaptor service is not currently available, retry later!")
                return json.dumps(response,indent=4)
            
            # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
            # restituiamo i dati desiderati
            urlTSA = dictionary['url']
            
            # Facciamo una get request per ottenere le quantità dei singoli prodotti
            # salvate su thingspeak
            r2 = requests.get(urlTSA + "/retrieveValues")
            dictionary2 = r2.json()
            
            # Inizializziamo a zero un contatore di prodotti in fase di terminazione
            n_prod=0
            message = 'The following items are running out:\n'
            prodList = []
            
            # Iteriamo tutti i prodotti restituiti da retrieveValues
            for product in dictionary2["update"]:
                # Se la quantità è al di sotto di 5, mandiamo un alert all'employee,
                # che si occuperà di riempire lo scaffale del relativo prodotto
                if int(product["Quantity"]) < 5:
                    prodList.append(product["ProductName"])
                    message += '- '+str(product["ProductName"])+' left: '+str(product["Quantity"]) +'. Aisle:'+str(product["Aisle"])+', Shelf: '+str(product["Shelf"])+'\n'
                    n_prod += 1
            response = {"Alert":message}
            responseTS = {"restockProducts" : prodList}
            # Se il contatore avrà un valore maggiore di zero significa che alcuni
            # prodotti stanno per terminare, quindi il servizio dovrà pubblicare 
            # un messaggio di alert. In caso contrario, nessun messaggio verrà pubblicato
            if n_prod>0:
                self.publish(self.topicBot,json.dumps(response,indent=4))
                self.publish(self.topicRestock,json.dumps(responseTS,indent=4))

    def publish(self, topic, message):
        self._paho_mqtt.publish(topic, message, 2)
                    
if __name__ == "__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # Inizializziamo il nostro oggetto EmployeeAlert e startiamolo
    updateTopic = dictionary["topic_thingspeak_updated"]
    topicBot = dictionary["topic_employees_bot"]
    topicTS = dictionary["topic_reader_store"]
    restockTopic = dictionary["topic_reader_store_restock"]
    pubID = "employeeAlert"
    alert=EmployeeAlert(pubID, updateTopic, topicBot, topicTS, restockTopic, dictionary['broker'], dictionary['port'])
    alert.start()
    
    while True:
        time.sleep(5)
        