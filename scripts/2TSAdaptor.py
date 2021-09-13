import cherrypy
import time
import json
import paho.mqtt.client as PahoMQTT
import requests

class RESTAdaptor(object):
    exposed=True

    def __init__(self, clientID, topic, broker, port):
        self.clientID= clientID 
        self.broker=broker
        self.port=port
        self.notifier = self
        self._paho_mqtt = PahoMQTT.Client(self.clientID,True)
        self.topics = topic
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived
        self.start()

    def myOnMessageReceived (self, paho_mqtt , userdata, msg):
        # A new message is received
        self.notify (msg.topic, msg.payload)

    def start(self):
        # Quando startiamo il RESTAdaptor, lo connettiamo al broker e lo
        # iscriviamo subito al topic del reader store. Ogni volta che 
        # thingspeak viene aggiornato, pubblica un messaggio su questo topic
        # avvisando così l'alert del cambiamento di quantità dei prodotti sugli 
        # scaffali
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()
        for topic in self.topics:
            self._paho_mqtt.subscribe(topic,0)

    def stop(self):
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))

    def notify(self,topic,payload):
        # Quando riceviamo un messaggio dal reader del negozio, lo salviamo nel payload
        payload = json.loads(payload)
        
        # Facciamo una richiesta al catalog per l'User API di Thingspeak,
        # in modo da poterci interfacciare con quest'ultimo
        r = requests.get(urlcatalog + "/TSUserAPI")
        dictionary = r.json()
        
        # Sfruttando l'User API, richiediamo tramite get request i channels
        # a nostra disposizione
        urlChannels = "https://api.thingspeak.com/channels.json?api_key="+dictionary['UserAPI']
        r2 = requests.get(urlChannels)
        dictionary2 = r2.json()
        
        if "restockProducts" in payload.keys():
            restockQuantity = 20
            for prod in payload["restockProducts"]:
                for channel in dictionary2:
                    # Quando troviamo la corrispondenza tra i due canali tramite nome 
                    # del prodotto, sovrascriviamo i dati di thingspeak, aggiornando le quantità
                    # dei prodotti in modo che corrispondano a quelle rilevate dal
                    # reader store
                    for idx,tag in enumerate(channel["tags"]):
                        if tag["name"] == prod:
                            API_write = channel['api_keys'][0]['api_key']
                            url = "https://api.thingspeak.com/update.json?api_key="+API_write+"&field"+str(idx+1)+"="+str(restockQuantity)  
                            r3 = requests.get(url)  
        else:
            # Iteriamo sui canali contenuti nel messaggio del reader store
            for payChannel in payload['e']:
                # E sui canali ottenuti direttamente da thingspeak
                for channel in dictionary2:
                    # Quando troviamo la corrispondenza tra i due canali tramite ID,
                    # sovrascriviamo i dati di thingspeak, aggiornando le quantità
                    # dei prodotti in modo che corrispondano a quelle rilevate dal
                    # reader store
                    if channel['id'] == payChannel['n']:
                        API_write = channel['api_keys'][0]['api_key']
                        url = "https://api.thingspeak.com/update.json?api_key="+API_write
                        for i,value in enumerate(payChannel['values']):
                            url += "&field"+str(i+1)+"="+str(value)  
                        r3 = requests.get(url)  

    def GET(self,*uri,**params):
        # Facciamo una richiesta al catalog per l'User API di Thingspeak,
        # in modo da poterci interfacciare con quest'ultimo
        r = requests.get(urlcatalog + "/TSUserAPI")
        userAPI = r.json()

        uri = uri[0]
        
        # Salviamo i parametri passati in input nella variabile lista
        if params!={}:
           valuesList = [params[key] for key in params.keys()]
           lista = []
           for el in valuesList:
                lista.append(el)
                
        # Splittiamo le possibili funzioni in base all'uri
        # In questo caso recuperiamo i canali a nostra disposizione, con relativo 
        # id, nome e fields (prodotti) contenuti al loro interno
        if uri == "getChannels":
            urlChannels = "https://api.thingspeak.com/channels.json?api_key="+userAPI['UserAPI']
            r2 = requests.get(urlChannels)
            channels = r2.json()
            
            # Inizializziamo una risposta vuota
            response = []
            
            # Per ogni canale recuperiamo id e nome
            for channel in channels:
                id = channel['id']
                name = channel['name']
                
                # I nomi dei fields sono stati opportunatamente salvati nelle tags
                # su thingspeak. Sfruttiamo questo fatto per recuperare l'indicazione
                # dei singoli prodotti
                fields = []
                for tag in channel['tags']:
                    fields.append(tag['name'])
                    
                # Creiamo un dizionario per ogni canale e appendiamolo alla risposta, 
                # che sarà quindi composta come una lista di dizionari
                data = {'id':id,'name':name,'fields':fields}
                response.append(data)
                
            return json.dumps(response, indent=4)
        
        # Quando richiamiamo il microservice productAvailability, esso si interfaccerà
        # con il TSAdaptor, al fine di recuperare i dati necessari allo svolgimento
        # del suo servizio. Tale adaptor si frappone tra service e thingspeak
        # in modo da rendere più scalabile il sistema
        elif uri == "productAvailability":
            # Dai parametri passati in input, recuperiamo categoria e prodotto
            # di cui vogliamo conoscere la disponibilità
            category = lista[0]
            product = lista[1].lower()
            
            # Facciamo la richiesta dei canali
            urlChannels = "https://api.thingspeak.com/channels.json?api_key="+userAPI['UserAPI']
            r2 = requests.get(urlChannels)
            channels = r2.json()
            
            # Inizializziamo un template di risposta
            response = {"Error": False,"lastUpdate":None,"quantity":None,"price":None}
            
            # Iterando sui canali, andiamo a trovare il prodotto di nostro interesse
            for channel in channels:
                # Se il nome del canale attuale è uguale alla categoria data 
                # come parametro, significa che ci troviamo nel canale giusto
                if channel["name"] == category:
                    ChannelID = channel["id"]
                    
                    # Carichiamo i metadata, che contengono al loro interno diverse
                    # informazioni, tra cui il prezzo, che andremo ad inserire nella
                    # risposta del microservice
                    metadata = json.loads(channel['metadata'])
                    
                    # Iteriamo su tutti i prodotti contenuti nel canale attuale
                    # sfruttando le tags
                    for field_ID,prod in enumerate(channel["tags"]):
                        # Una volta che troviamo il prodotto da noi cercato
                        if product==prod["name"]:
                            # Possiamo inizializzare l'url della richiesta da effettuare
                            # a thingspeak per sapere la sua disponibilità
                            urlproduct = "https://api.thingspeak.com/channels/"+str(ChannelID)+"/fields/"+str(field_ID+1)+"/last.json"
                            r3 = requests.get(urlproduct)
                            r3 = r3.json()
                            
                            # E aggiornare il template response, da restituire in
                            # output
                            response["lastUpdate"] = r3["created_at"]
                            response["quantity"] = r3["field"+str(field_ID+1)]
                            response["price"] = str(metadata["price"+str(field_ID+1)])
                            
                            return json.dumps(response,indent=4)
            
            # Nel caso in cui non si trovi in nessun canale il prodotto desiderato, 
            # restituiremo nella response una chiave error settata a True, in 
            # modo da restituire il corrispondente messaggio di non disponibilità
            # del prodotto
            response["Error"] = True
            
            return json.dumps(response,indent=4)

        # In questo caso restituiamo i prezzi di una lista di prodotti passati 
        # come parametro
        elif uri == "getPrices":
            # Salviamo i prodotti passati come parametro
            products = lista[0]
            
            # Recuperiamo i canali thingspeak
            urlChannels = "https://api.thingspeak.com/channels.json?api_key="+userAPI['UserAPI']
            r2 = requests.get(urlChannels)
            channels = r2.json()
            
            # Inizializziamo un template della risposta
            response = {"products":[],"prices":[]}
            
            # Iteriamo sui prodotti dati in input
            for product in products:
                # E cerchiamoli nei canali sfruttando le tags
                for channel in channels:
                    metadata = json.loads(channel['metadata'])
                    for field_ID,prod in enumerate(channel["tags"]):
                        # Quando troviamo il prodotto desiderato, inseriamolo
                        # nella risposta insieme al relativo prezzo
                        if product == prod["name"]:
                            response["products"].append(product)
                            response["prices"].append(str(metadata["price"+str(field_ID+1)]))
                  
            return json.dumps(response,indent=4)
        
        # Quando richiamiamo il microservice productPosition, esso si interfaccerà
        # con il TSAdaptor, al fine di recuperare i dati necessari allo svolgimento
        # del suo servizio. Tale adaptor si frappone tra service e thingspeak
        # in modo da rendere più scalabile il sistema
        elif uri == "productPosition":
            # Dai parametri passati in input, recuperiamo categoria e prodotto
            # di cui vogliamo conoscere la disponibilità
            category = lista[0]
            product = lista[1].lower()
            
            # Recuperiamo i canali thingspeak
            urlChannels = "https://api.thingspeak.com/channels.json?api_key="+userAPI['UserAPI']
            r2 = requests.get(urlChannels)
            channels = r2.json()
            
            # Inizializziamo un template della risposta
            response = {"Error": False, "Aisle":None,"Shelf":None}
            
            # Iteriamo sui canali
            for channel in channels:
                # Quando individuiamo il canale corrispondente alla categoria
                # del prodotto dato come parametro
                if channel["name"] == category:
                    ChannelID = channel["id"]
                    
                    # Ricerchiamo il prodotto specifico sfruttando le tags
                    for field_ID,prod in enumerate(channel["tags"]):
                        # Una volta trovato il prodotto
                        if product==prod["name"]:
                            # Carichiamo i metadata, nei quali abbiamo salvato
                            # corsia e scaffale del posizionamento del prodotto
                            # cercato, e inseriamoli nella risposta
                            metadata = json.loads(channel['metadata'])
                            
                            aisle = metadata['aisle']
                            shelf = metadata["field"+str(field_ID+1)]
                            
                            response["Aisle"] = aisle
                            response["Shelf"] = shelf
                            
                            return json.dumps(response,indent=4)
            
            # Nel caso in cui non si trovi in nessun canale il prodotto desiderato, 
            # restituiremo nella response una chiave error settata a True, in 
            # modo da restituire il corrispondente messaggio di non disponibilità
            # del prodotto
            response["Error"] = True
            
            return json.dumps(response,indent=4)
        
        # In questo caso vogliamo recuperare le quantità di tutti i prodotti
        # di tutte le categorie, con relativo posizionamento, in modo da aggiornare
        # i dipendenti su eventuali restock
        elif uri == "retrieveValues":
            # Richiediamo i canali thingspeak
            urlChannels = "https://api.thingspeak.com/channels.json?api_key="+userAPI['UserAPI']
            r2 = requests.get(urlChannels)
            channels = r2.json()
            
            # Inizializziamo un template della risposta
            response = {"update":[]}
            
            # Iteriamo sui canali
            for channel in channels:
                # Iteriamo su ogni prodotto contenuto all'interno di questo canale
                for field_ID,prod in enumerate(channel["tags"]):
                    # Inizializziamo un template del singolo prodotto
                    template = {"Category":channel["name"],"ProductName":None, "Quantity": None, "Aisle":None, "Shelf":None}
                    
                    # Facciamo una richiesta a Thingspeak per quello specifico
                    # prodotto
                    urlproduct = "https://api.thingspeak.com/channels/"+str(channel["id"])+"/fields/"+str(field_ID+1)+"/last.json"
                    r3 = requests.get(urlproduct)
                    r3 = r3.json()
                    
                    # Salviamo le sue informazioni nel template
                    template["ProductName"] = prod["name"]
                    template["Quantity"] = r3["field"+str(field_ID+1)]
                    
                    metadata = json.loads(channel['metadata'])
                    template["Aisle"] = metadata['aisle']
                    template["Shelf"] = metadata["field"+str(field_ID+1)]
                     
                    # Appendiamolo alla risposta in modo da avere una lista di
                    # prodotti con tutte le informazioni necessarie
                    response["update"].append(template)

            return json.dumps(response,indent=4)
        


if __name__=="__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()

    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tool.session.on':True        
            }    
        }   
    cherrypy.config.update({'server.socket_port':8050}) 
    cherrypy.tree.mount(RESTAdaptor("MarketTSAdaptor",[dictionary["topic_reader_store"]],dictionary['broker'],dictionary['port']),'/',conf)
    cherrypy.engine.start()

    # Ogni 5 minuti, inviamo una post request al catalog per comunicare che 
    # il servizio è online, con il relativo URL al quale è raggiungibile
    while True:
        payload = {"ID" : "TSAdaptor", "url" : "http://127.0.0.1:8050"}
        payload = json.dumps(payload,indent=4)
        requests.post(urlcatalog + "/updateOnlineService",data=payload)
        time.sleep(300)
