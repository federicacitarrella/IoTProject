import telepot 
from telepot.loop import MessageLoop 
import json
import requests
import time
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
import paho.mqtt.client as PahoMQTT
from collections import Counter
import pandas as pd
from io import BytesIO

class BotMarket:
    def __init__(self,token,client_ID,broker,port,topic):
        self.broker=broker
        self.port=port
        self.client_ID=client_ID
        self.topic=topic
        self._paho_mqtt = PahoMQTT.Client(client_ID, True)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

        # Salviamo il token del BOT, dato in input dall'utente
        self.tokenBot=token
        
        # Let's start the bot
        self.bot=telepot.Bot(self.tokenBot)
        
        # Quando riceviamo un messaggio, sarà gestito dal metodo on_chat_message
        MessageLoop(self.bot, {'chat': self.on_chat_message,'callback_query': self.on_callback_query}).run_as_thread()
        
        # Carichiamo l'URL del catalog dal file json a nostra disposizione
        self.urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
        
        # Inizializziamo una lista vuota di clients, che verrà riempita
        # volta per volta
        self.clients = []
        self.chat_ID_list_clients = []
        
        # Facciamo una get request al catalog, per caricare i dipendenti
        # autenticati e salviamo i loro chat ID in una lista 
        r_employees = requests.get(self.urlcatalog + "/GetEmployees")
        dictionary_GE = r_employees.json()
        self.chat_ID_list_employees = dictionary_GE["employees"]
        
        # Inizializziamo una lista vuota di dipendenti, se tramite la get request 
        # precedente abbiamo ricevuto qualche chat ID, inseriamolo in tale lista
        self.employees = []
        for chat_ID in self.chat_ID_list_employees:
            employee= {'chat_ID': chat_ID, 'message':'','status':1}
            self.employees.append(employee)

    def start(self):
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()
        self._paho_mqtt.subscribe(self.topic, 2)

    def stop(self):
        self._paho_mqtt.unsubscribe(self.topic) 
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))

    def myOnMessageReceived(self, paho_mqtt , userdata, msg):
        # A new message is received
        self.notify(msg.topic, msg.payload)
        
    def notify(self,topic,payload):
        # Leggiamo il payload e salviamolo in una variabile locale
        payload = json.loads(payload)
        
        # Ogni volta che riceviamo un messaggio via MQTT, significa che uno o più
        # prodotti sugli scaffali stanno terminando. Vogliamo notificare questo
        # evento ai dipendenti, in modo che possano subito adoperarsi per 
        # rifornire gli scaffali prima che questi diventino vuoti.
        # Mandiamo un messaggio a tutti i dipendenti autenticati e salvati
        # in self.chat_ID_list_employees dove comunichiamo i prodotti che stanno
        # terminando e la loro collocazione all'interno dello store
        for employee_chat_ID in self.chat_ID_list_employees:
            self.bot.sendMessage(employee_chat_ID, text=payload["Alert"])

    def on_chat_message(self,msg):
        # La funzione glance ottiene dal msg (json) il content_type (foto, testo, video...),
        # il chat type (private, groups) e il chat ID
        content_type,chat_type,chat_ID=telepot.glance(msg)
        
        # Se il chat ID non è contenuto nella lista dei dipendenti o dei clienti,
        # significa che l'utente sta interagendo con il bot per la prima volta,
        # di conseguenza vogliamo "registrarlo" alla sua categoria di appartenenza
        if chat_ID not in self.chat_ID_list_employees and chat_ID not in self.chat_ID_list_clients:
            # Se il messaggio inviato è "/start", comparirà un messaggio a schermo
            # che da la possibilità all'utente di selezionare la sua categoria di 
            # appartenenza tramite pulsante (employee o customer)
            if msg["text"]=="/start":
                # Una volta che l'utente seleziona il pulsante, il codice proseguirà
                # nella parte relativa agli on_callback_query
                buttons=[[InlineKeyboardButton(text='Employee', callback_data='Employee')],[InlineKeyboardButton(text='Customer', callback_data='Customer')]]
                keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)            
                self.bot.sendMessage(chat_ID, text='Are you a: ', reply_markup=keyboard)                
        
        # Se il chat_ID è contenuto all'interno della lista degli impiegati
        elif chat_ID in self.chat_ID_list_employees:
            # Se il chat_ID è contenuto nella lista dei dipendenti, troviamo il chat
            # ID corretto salvato nella lista e salviamo nel dizionario del dipendente
            # attuale il messaggio inviato tramite bot (in questo modo possiamo
            # gestire più utenti contemporaneamente evitando che si scambino
            # messaggi/risposte del bot tra più utenti)
            for i,employee in enumerate(self.employees):
                if chat_ID == employee["chat_ID"]:
                    employee["message"] = msg["text"]
                    
                    # In modo da differenziare le varie operazioni da svolgersi, è stato
                    # introdotto l'uso dello "status". Ogni utente ha un suo dizionario,
                    # che contiene tra le altre cose (chat_ID, message ecc.) uno status.
                    # In base alla funzione che vogliamo eseguire, assegniamo uno specifico
                    # valore di status del dizionario dell'utente corrente.
                    
                    # Se lo status è zero, il dipendente si deve ancora autenticare,
                    # quindi il messaggio che ha inviato corrisponde alla pwd
                    if employee["status"] == 0:
                        # Creiamo un payload contenente la password
                        payload = {"pass" : employee["message"]}
                        
                        # E facciamo la get request al servizio CheckPassword
                        # contenuto nel catalog
                        r_pw = requests.get(self.urlcatalog + "/CheckPassword", params=payload)
                        dictionary_pw = r_pw.json()
                        
                        # Se la richiesta ha avuto come esito "Success", la pwd
                        # inserita era corretta
                        if dictionary_pw["Success"]:
                            # Confermiamo al dipendente l'avvenuto log-in 
                            self.bot.sendMessage(employee['chat_ID'], text='Log-in successfully completed')
                            
                            # Assegniamogli lo status 1, che significa che fa qui
                            # in avanti potrà fare le operazioni pensate per i 
                            # dipendenti
                            self.employees[i]["status"] = 1
                            
                            # Creiamo un payload con la lista aggiornata dei dipendenti
                            # e, tramite post request, aggiorniamo il catalog
                            payload = {"employees":self.chat_ID_list_employees}
                            payload = json.dumps(payload,indent=4)
                            requests.post(self.urlcatalog + "/updateEmployees",data=payload)
                        
                        # Se la pwd è sbagliata
                        else:
                            # Comunichiamolo all'utente, rimuoviamolo contestualmente
                            # dalla lista dei dipendenti
                            self.bot.sendMessage(employee['chat_ID'], text='Wrong password!')
                            self.chat_ID_list_employees.remove(employee['chat_ID'])
                            self.employees.pop(i)
                            
                            # Re-inizializziamo il ciclo, chiedendogli la sua
                            # categoria di appartenenza 
                            buttons=[[InlineKeyboardButton(text='Employee', callback_data='Employee')],[InlineKeyboardButton(text='Customer', callback_data='Customer')]]
                            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)            
                            self.bot.sendMessage(employee['chat_ID'], text='Are you a: ', reply_markup=keyboard) 
                    
                    # Se lo status è uguale a 1, il dipendente si è già autenticato
                    elif employee["status"] == 1:
                        # Nel caso in cui il messaggio sia "start", diamo il benvenuto
                        # all'employee
                        if employee["message"]=="/start":
                            self.bot.sendMessage(employee['chat_ID'], text='Welcome employee!!!') 
                        # Se invece desidera fare il logout
                        if employee["message"]=="/logout":
                            # Rimuoviamolo dalla lista degli impiegati
                            self.chat_ID_list_employees.remove(employee['chat_ID'])
                            self.employees.pop(i)
                            
                            # Generiamo il payload contenente la lista aggiornata
                            # e inviamolo tramite post request al catalog, in modo
                            # che venga aggiornato anche il json del catalog
                            payload = {"employees":self.chat_ID_list_employees}
                            payload = json.dumps(payload,indent=4)
                            requests.post(self.urlcatalog + "/updateEmployees",data=payload)
                            
                            # Comunichiamo all'utente l'avvenuto log-out
                            self.bot.sendMessage(employee['chat_ID'], text='Successful logout') 

        # Se il chat ID è contenuto nella lista dei customers, avviamo tutta la 
        # parte di codice relativa ai clienti        
        elif chat_ID in self.chat_ID_list_clients:
            
            # Iteriamo su tutti i clienti, in modo da trovare il cliente attuale
            # grazie alla corrispondenza dei chat_ID (in modo da evitare confusioni
            # dovute a messaggi contemporanei da parte di più utenti)
            for i,client in enumerate(self.clients):
                
                # Una volta trovato il giusto cliente, salviamo nel suo dizionario
                # il messaggio appena inviato dal bot
                if chat_ID == client['chat_ID']:
                    client["message"] = msg["text"]
                    
                    # Se lo status è 0, il cliente si è appena identificato come
                    # tale, quindi è pronto ad effettuare qualsiasi richiesta
                    # tra quelle disponibili nella lista del bot
                    if client["status"]==0: 
                        
                        # Se il messaggio è start, il bot darà il benvenuto al cliente,
                        # riconoscendolo come tale
                        if client["message"]=="/start":
                            self.bot.sendMessage(client['chat_ID'], text='Welcome customer!!!') 
                        
                        # Se vuole scoprire la disponibilità di un prodotto
                        elif client["message"]=="/productfinder":
                            # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                            # vogliamo raggiungere
                            payload = {"ID" : "TSAdaptor"}
        
                            # Tramite get request contattiamo il catalog, chiedendo se il microservice
                            # desiderato è online e, in tal caso, ricevendo il suo URL
                            r = requests.get(self.urlcatalog + "/getEndPoints", params=payload)
                            dictionary = r.json()
        
                            # Se non è online al momento, printiamolo e restituiamolo come risposta 
                            # a chiunque richieda i suoi servizi
                            if not dictionary["exist"]:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                                return
        
                            # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
                            # restituiamo i dati desiderati
                            urlTSA = dictionary['url']
                            
                            # Iniziamo un try except; i servizi sono sottoposti
                            # continuamente a rimozione quando non si "aggiornano" dopo
                            # tot tempo. Talvolta, però, potrebbero andare offline
                            # prima di esser rimossi, portando ad un errore del sistema
                            # e allo stop del bot. Sfruttando il try except, invece,
                            # siamo in grado di mantenere il bot up and running
                            # anche nel caso in cui il servizio sia al momento
                            # non disponibile 
                            try:
                                # Effettuiamo una richiesta dei canali al Thingspeak
                                # Adaptor
                                r2 = requests.get(urlTSA+'/getChannels')
                                channels = r2.json()
                                
                                # Inizializziamo una lista vuota di pulsanti
                                buttons=[]
                                
                                # Per ogni canale contenuto in Thingspeak creiamo
                                # un pulsante apposito. In questo modo, quando un utente
                                # vuole cercare un prodotto, dovrà innanzitutto
                                # selezionare la categoria di appartenenza in seguito
                                # alla richiesta ricevuta sul bot
                                for channel in channels:
                                    buttons.append([InlineKeyboardButton(text=str(channel['name']), callback_data=str(channel['name']))])
                                keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)            
                                self.bot.sendMessage(client['chat_ID'], text='Select a category', reply_markup=keyboard)
                                
                                # Settiamo lo status del cliente attuale ad 1, in modo
                                # da gestire questa routine nell'on_callback_query
                                self.clients[i]["status"]=1
                            
                            # Se per qualche motivo la parte di codice contenuta 
                            # nel try non dovesse andare a buon fine, mandiamo un 
                            # messaggio all'utente segnalandogli l'impossibilità di 
                            # sfruttare il servizio richiesto al momento
                            except:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                                
                        # Se vuole individuare la posizione di un prodotto
                        elif client["message"]=="/productposition":
                            # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                            # vogliamo raggiungere
                            payload = {"ID" : "TSAdaptor"}
                            
                            # Tramite get request contattiamo il catalog, chiedendo se il microservice
                            # desiderato è online e, in tal caso, ricevendo il suo URL
                            r = requests.get(self.urlcatalog + "/getEndPoints", params=payload)
                            dictionary = r.json()
                            
                            # Se non è online al momento, printiamolo e restituiamolo come risposta 
                            # a chiunque richieda i suoi servizi
                            if not dictionary["exist"]:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                                return
                            
                            # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
                            # restituiamo i dati desiderati
                            urlTSA = dictionary['url']
                           
                            # Try-except per non far fermare il bot in caso 
                            # di errore
                            try:
                                # Effettuiamo una richiesta dei canali al Thingspeak
                                # Adaptor
                                r2 = requests.get(urlTSA+'/getChannels')
                                channels = r2.json()
                                
                                # Inizializziamo una lista vuota di pulsanti
                                buttons=[]
                                
                                # Per ogni canale contenuto in Thingspeak creiamo
                                # un pulsante apposito. In questo modo, quando un utente
                                # vuole cercare un prodotto, dovrà innanzitutto
                                # selezionare la categoria di appartenenza in seguito
                                # alla richiesta ricevuta sul bot
                                for channel in channels:
                                    buttons.append([InlineKeyboardButton(text=str(channel['name']), callback_data=str(channel['name']))])
                                keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)            
                                self.bot.sendMessage(client['chat_ID'], text='Select a category:', reply_markup=keyboard)
                                
                                # Settiamo lo status del cliente attuale a 2, in modo
                                # da gestire questa routine nell'on_callback_query
                                self.clients[i]["status"]=2
                                
                            # Se il servizio non fosse disponibile al momento,
                            # segnaliamolo all'utente
                            except:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                                
                        # Se il cliente vuole consultare gli sconti disponibili
                        elif client["message"]=="/checkdiscounts":
                            # Chiediamo al cliente che tipologia di sconti vuole
                            # visualizzare tra "prodotti singoli" e "prodotti a coppie"
                            buttons=[[InlineKeyboardButton(text='Single Products', callback_data='SingleProducts')],[InlineKeyboardButton(text='Coupled Products', callback_data='CoupledProducts')]]
                            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                            
                            self.bot.sendMessage(client['chat_ID'], text='Offers type:', reply_markup=keyboard)
                            
                            # Settiamo lo status del cliente attuale a 3, in modo
                            # da gestire questa routine nell'on_callback_query
                            self.clients[i]["status"]=3
                        
                        # Se il cliente vuole consultare l'affollamento del negozio
                        elif client["message"]=="/timedistribution":
                            # Chiediamo all'utente se vuole consultare gli orari
                            # di un giorno specifico o dell'intera settimana
                            buttons=[[InlineKeyboardButton(text='Specific Day', callback_data='specific')],[InlineKeyboardButton(text='Whole week', callback_data='week')]]
                            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                            
                            self.bot.sendMessage(client['chat_ID'], text='What kind of time statistics are you interested in?', reply_markup=keyboard)
                            
                            # Settiamo lo status del cliente attuale a 4, in modo
                            # da gestire questa routine nell'on_callback_query
                            self.clients[i]["status"]=4
                         
                        # Se il cliente vuole effettuare il logout
                        elif client["message"]=="/logout":
                            # Rimuoviamolo dalla lista dei clienti e comunichiamogli
                            # l'avvenuto log-out
                            self.chat_ID_list_clients.remove(client['chat_ID'])
                            self.clients.pop(i)
                            self.bot.sendMessage(client['chat_ID'], text='Successful logout') 
                        
                        # Se il cliente inserisce un comando non esistente, comunichiamoglielo
                        else:
                            self.bot.sendMessage(chat_ID,text="Command not supported")
                        
                    # Continua la routine relativa al processo di product finder
                    elif client["status"] == 1:
                        
                        # Il cliente dovrebbe aver inviato il nome del prodotto,
                        # che salviamo quindi nel suo dizionario sotto la chiave 
                        # "product"
                        client["product"] = client["message"]
                        self.clients[i]["product"] = client["product"]

                        # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                        # vogliamo raggiungere
                        payload = {"ID" : "ProdAvailability"}
                        
                        r_PA= requests.get(self.urlcatalog + "/getEndPoints",params=payload)
                        dictionary_PA = r_PA.json()

                        # Se non è online al momento, printiamolo e restituiamolo come risposta 
                        # a chiunque richieda i suoi servizi
                        if not dictionary_PA["exist"]:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                            return
                        
                        # Nel caso in cui fosse online, invece, salviamone l'url
                        url = dictionary_PA["url"]
                        
                        # Generiamo il payload contenente la categoria e il nome
                        # del prodotto desiderato
                        payload = {'category': self.clients[i]["category"], 'product': self.clients[i]["product"]}
                        
                        # Try-Except per prevenire errori dovuti al servizio
                        # inaspettatamente offline
                        try:
                            # Facciamo richiesta al servizio, dando come parametro
                            # il payload precedentemente generato
                            r2 = requests.get(url,params=payload)  
                            dictionary2 = r2.json()
                            
                            # Se otteniamo come risposta un messaggio di errore,
                            # significa che l'utente ha inserito un prodotto 
                            # che non esiste nel nostro database. Comunichiamoglielo
                            if dictionary2["Error"]:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the item you\'re looking for doesn\'t exist!')
                            
                            # Nel caso in cui non vi sia alcun errore
                            else:
                                
                                # Salviamo la data e l'ora dell'ultimo aggiornamento
                                # del prodotto richiesto
                                date_time=str(dictionary2["lastUpdate"]).split('T')
                                date_time[1]=date_time[1].replace('Z','')
                                
                                # Se la quantità del prodotto è pari a zero,
                                # comunichiamo al cliente che il prodotto
                                # è esaurito
                                if int(dictionary2['quantity'])==0: 
                                    self.bot.sendMessage(client['chat_ID'],text='We are sorry, this item is currently out of stock 🥺')
                                
                                # In caso contrario, comunichiamo quantità, prezzo
                                # e ora del "check": 
                                # es. "2 pizze disponibili. Prezzo: 5.00 $.
                                # Ultimo aggiornamento: 9:00 04/09/2021"
                                else:
                                    self.bot.sendMessage(client['chat_ID'],text=str(dictionary2["quantity"])+" "+str(self.clients[i]["product"]).lower()+" left.\nProduct price: €" + str(format(float(dictionary2["price"]),'.2f')) + "\nLast update: "+ str(date_time[0].split('-')[2])+'-'+ str(date_time[0].split('-')[1])+'-'+ str(date_time[0].split('-')[0]) +', '+str(int(date_time[1].split(':')[0])+2)+':'+str(date_time[1].split(':')[1])+':'+str(date_time[1].split(':')[2]))
                        
                        # Nel caso in cui non fosse disponibile il servizio, 
                        # comunichiamolo al cliente
                        except:
                            self.bot.sendMessage(client['chat_ID'],text='Sorry, the service is not currently available, retry later! 🥺')
                        
                        # Settiamo in entrambi i casi del try-except lo status
                        # del cliente a zero, in modo che sia pronto ad effettuare
                        # una nuova richiesta
                        finally:    
                            self.clients[i]["status"] = 0
                            
                    # Continua la routine relativa al processo di product finder
                    elif client["status"] == 2:
                        
                        # Il cliente dovrebbe aver inviato il nome del prodotto,
                        # che salviamo quindi nel suo dizionario sotto la chiave 
                        # "product"
                        client["product"] = client["message"]
                        self.clients[i]["product"] = client["product"]

                        # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                        # vogliamo raggiungere
                        payload = {"ID" : "ProdPosition"}
                        
                        r_PP = requests.get(self.urlcatalog + "/getEndPoints",params=payload)
                        dictionary_PP = r_PP.json()

                        # Se non è online al momento, printiamolo e restituiamolo come risposta 
                        # a chiunque richieda i suoi servizi
                        if not dictionary_PP["exist"]:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                            return

                        # Nel caso in cui fosse online, invece, salviamone l'url
                        url = dictionary_PP["url"]
                        
                        # Generiamo il payload contenente la categoria e il nome
                        # del prodotto desiderato
                        payload = {'category': client["category"], 'product': client["product"]}
                        
                        # Try-Except per prevenire errori dovuti al servizio
                        # inaspettatamente offline
                        try:
                            # Facciamo richiesta al servizio, dando come parametro
                            # il payload precedentemente generato
                            r2 = requests.get(url,params=payload)  
                            dictionary2 = r2.json()
                            
                            # Se otteniamo come risposta un messaggio di errore,
                            # significa che l'utente ha inserito un prodotto 
                            # che non esiste nel nostro database. Comunichiamoglielo
                            if dictionary2["Error"]:
                                self.bot.sendMessage(client['chat_ID'],text='Sorry, the item you\'re looking for doesn\'t exist!')
                            
                            # In caso contrario, comunichiamo la posizione del prodotto
                            # cercato:
                            # es. "Puoi trovare latte in Corsia: 1, Scaffale: 10" 
                            else:
                                self.bot.sendMessage(client['chat_ID'],text='You can find ' +str(self.clients[i]["product"]) + ' in:\nAisle number: ' + dictionary2['Aisle'] + ' \nShelf: ' + dictionary2['Shelf'])
                        
                        # Nel caso in cui non fosse disponibile il servizio, 
                        # comunichiamolo al cliente
                        except:
                            self.bot.sendMessage(client['chat_ID'],text='Sorry, the service is not currently available, retry later! 🥺')
                        
                        # Settiamo in entrambi i casi del try-except lo status
                        # del cliente a zero, in modo che sia pronto ad effettuare
                        # una nuova richiesta
                        finally:    
                            self.clients[i]["status"] = 0
                            

        
    def on_callback_query(self,msg): 
        query_ID , chat_ID , query_data = telepot.glance(msg,flavor='callback_query') 
        
        # Se il chat ID non è registrato nella lista dei customers e degli impiegati,
        # significa che è il primo accesso al bot dell'utente, che si sta dunque
        # identificando
        if chat_ID not in self.chat_ID_list_employees and chat_ID not in self.chat_ID_list_clients:
            # Se l'utente ha selezionato il pulsante customer, creiamo il suo dizionario
            # client con chat_ID, message, status ecc. e appendiamolo alla lista
            # di clients che comprende tutti i customers del negozio, mandando
            # un messaggio di benvenuto al cliente che potrà così interagire
            # tramite il bot con tutte le funzioni pensate per il cliente
            if query_data == "Customer":
                client= {'chat_ID': chat_ID, 'message':'','status':0,'category':None,'product':None, 'offertype':None, 'timetype':None}
                self.clients.append(client)
                self.chat_ID_list_clients.append(chat_ID)
                self.bot.sendMessage(chat_ID, text='Welcome!')
             
            # Se l'utente si è identificato come dipendente, invece, inseriamolo
            # momentaneamente nella lista dei dipendenti e chiediamogli la password
            # per autenticarsi (pwd idealmente fornita dal direttore del supermarket
            # al momento dell'assunzione)
            elif query_data == "Employee":
                employee= {'chat_ID': chat_ID, 'message':'','status':0}
                self.employees.append(employee)
                self.chat_ID_list_employees.append(chat_ID)
                self.bot.sendMessage(chat_ID, text='Insert employee password:')
        
        # Se il chat ID è contenuto nella lista dei customers, proseguiamo con
        # le operazioni relative ad essi
        elif chat_ID in self.chat_ID_list_clients:
            # Identifichiamo il customer attuale tramite chat_ID
            for i,client in enumerate(self.clients):
                if chat_ID == client['chat_ID']:
                    
                    # Se lo status è uguale a 1, siamo nella routine relativa
                    # al product finder
                    if client["status"]==1:
                        # Salviamo nel dizionario dell'utente attuale, sotto la 
                        # chiave category, il callback del pulsante premuto 
                        # dall'utente, a cui era stato chiesto di selezionare
                        # la categoria di appartenenza del prodotto desiderato
                        self.clients[i]["category"]=query_data
                        
                        # Chiediamo all'utente il nome del prodotto che si vuole trovare
                        self.bot.sendMessage(chat_ID,text="Write the name of the product you want to find:")
                    
                    # Se lo status è uguale a 2, siamo nella routine relativa
                    # al product position
                    elif client["status"] == 2:
                        # Salviamo in category il callback del pulsante premuto
                        self.clients[i]["category"]=query_data
                        
                        # Chiediamo all'utente il nome del prodotto che si vuole trovare
                        self.bot.sendMessage(chat_ID,text="Write the name of the product you want to localize:")
                    
                    # Se lo status è uguale a 3, siamo nella routine relativa
                    # agli sconti
                    elif client["status"] == 3:
                        # Salviamo la tipologia di sconti richiesta dal cliente
                        self.clients[i]["offertype"]=query_data

                        # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                        # vogliamo raggiungere
                        payload = {"ID" : "CheckDiscounts"}

                        r_CD = requests.get(self.urlcatalog + "/getEndPoints",params=payload)
                        dictionary_CD = r_CD.json()

                        # Se non è online al momento, printiamolo e restituiamolo come risposta 
                        # a chiunque richieda i suoi servizi
                        if not dictionary_CD["exist"]:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                            return
                        
                        # Nel caso in cui fosse online, invece, salviamone l'url
                        url = dictionary_CD["url"]
                        
                        # Generiamo il payload contenente il tipo di offerta desiderato
                        payload = {'offertype': client["offertype"]}
                        
                        # Try-Except per prevenire errori dovuti al servizio
                        # inaspettatamente offline
                        try:
                            # Facciamo richiesta al servizio, dando come parametro
                            # il payload precedentemente generato
                            r2 = requests.get(url + "/CheckDiscounts",params=payload)  
                            dictionary2 = r2.json()
                            
                            # Se è contenuto "error" nelle chiavi della risposta,
                            # significa che il processo non ha avuto un esito positivo.
                            # Mandiamo all'utente il messaggio di errore che
                            # spiega cosa è andato storto
                            if "error" in dictionary2.keys():
                                self.bot.sendMessage(chat_ID,text=dictionary2["error"])
                                
                            # Se non ci son stati problemi
                            else:
                                # Se siamo nel caso di offerte singole
                                if self.clients[i]["offertype"] == 'SingleProducts':
                                    
                                    # Salviamo in discounted i prodotti scontati
                                    discounted = dictionary2["products"]
                                    
                                    # Inizializziamo una risposta, a cui appenderemo
                                    # volta per volta i prodotti scontati
                                    resp = 'The following items are discounted:\n\n'
                                    
                                    # Splittiamo i prodotti, che vengono restituiti
                                    # dal servizio checkDiscounts con relativo 
                                    # prezzo base e prezzo scontato
                                    for el in discounted:
                                        parts = el.split("/")
                                        resp += parts[0].upper() + '\nBase price: € ' + parts[1] + '\nDiscounted price: € ' + parts[2] + '\n\n'
                                    self.bot.sendMessage(chat_ID,text=resp)
                                
                                # Se siamo nel caso di offerte singole
                                elif self.clients[i]["offertype"] == 'CoupledProducts':
                                    # Salviamo in discounted i prodotti scontati
                                    discounted = dictionary2["products"]
                                    
                                    # Inizializziamo una risposta, a cui appenderemo
                                    # volta per volta i prodotti scontati
                                    resp = 'If you buy all the products belonging to group 1 and 2 you will get the second group items at 50%: \n'
                                    
                                    # Generiamo la risposta e mandiamola all'utente
                                    for couple in discounted:
                                        ant = ", ".join(couple["antecedents"])
                                        cons = ", ".join(couple["consequents"])
                                        resp += "Group 1: " + ant + ".\t Group 2: " + cons + "\n"
                                        
                                    self.bot.sendMessage(chat_ID,text=resp)
                        
                        # Nel caso in cui non fosse disponibile il servizio, 
                        # comunichiamolo al cliente
                        except:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                        
                        # Settiamo in entrambi i casi del try-except lo status
                        # del cliente a zero, in modo che sia pronto ad effettuare
                        # una nuova richiesta
                        finally:
                            self.clients[i]["status"] = 0
                      
                    # Se lo status è uguale a 4, siamo nella routine relativa
                    # al time distribution
                    elif client["status"] == 4:
                        # Salviamo la tipologia di time distribution richiesta dal cliente
                        self.clients[i]["timetype"]=query_data
                        
                        # Se la richiesta è su base settimanale
                        if client["timetype"] == 'week':

                            # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                            # vogliamo raggiungere
                            payload = {"ID" : "TimeAnalysis"}

                            r_CD = requests.get(self.urlcatalog + "/getEndPoints",params=payload)
                            dictionary_CD = r_CD.json()

                            # Se non è online al momento, printiamolo e restituiamolo come risposta 
                            # a chiunque richieda i suoi servizi
                            if not dictionary_CD["exist"]:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                                return
                            
                            # Nel caso in cui fosse online, invece, salviamone l'url
                            url = dictionary_CD["url"]
                            
                            # Generiamo il payload contenente il tipo di timedistribution
                            # desiderato
                            payload = {'timetype': client["timetype"]}
                            
                            # Try-Except per prevenire errori dovuti al servizio
                            # inaspettatamente offline
                            try:
                                # Facciamo richiesta al servizio, dando come parametro
                                # il payload precedentemente generato
                                r2 = requests.get(url + "/TimeDistribution",params=payload)  
                                outList = r2.json()
                                
                                # Contiamo il numero di occurrence ottenuti in output
                                data = Counter(outList)
                                
                                # Salviamo i dati in un dataframe pandas
                                df = pd.DataFrame.from_dict(data, orient='index')
                                
                                # Try-Except per prevenire errori dovuti a dati
                                # in output non sufficienti per creare il plot
                                try:
                                    # Generiamo il plot a barre
                                    plot = df.plot(kind='bar')
                                    
                                    # Creiamo la figure relativa e mandiamola
                                    # all'utente
                                    fig = plot.get_figure()
                                    buffer = BytesIO()
                                    fig.savefig(buffer, format='png')
                                    buffer.seek(0)
                                    self.bot.sendPhoto(chat_id=client['chat_ID'], photo=buffer)
                                
                                # Se i dati collezionati non sono sufficienti
                                # a creare il plot, avvertiamo l'utente 
                                except:
                                    self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the requested data are not already collected, retry later!')
                            
                            # Nel caso in cui non fosse disponibile il servizio, 
                            # comunichiamolo al cliente
                            except:
                                self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                            
                            # Settiamo in entrambi i casi del try-except lo status
                            # del cliente a zero, in modo che sia pronto ad effettuare
                            # una nuova richiesta
                            finally:
                                self.clients[i]["status"] = 0
                        
                        # Invece se la richiesta è di un giorno preciso
                        else:
                            # Chiediamo all'utente quale giorno della settimana 
                            # desidera visualizzare
                            buttons=[[InlineKeyboardButton(text='Monday', callback_data='mon')],[InlineKeyboardButton(text='Tuesday', callback_data='tue')],[InlineKeyboardButton(text='Wednesday', callback_data='wed')],[InlineKeyboardButton(text='Thursday', callback_data='thu')],[InlineKeyboardButton(text='Friday', callback_data='fri')],[InlineKeyboardButton(text='Saturday', callback_data='sat')],[InlineKeyboardButton(text='Sunday', callback_data='sun')]]
                            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                            self.bot.sendMessage(chat_ID,text="Choose the desired day:", reply_markup=keyboard)
                            
                            # Settiamo lo status del cliente attuale a 5, in modo
                            # da gestire questa routine nell'on_callback_query
                            self.clients[i]["status"]=5
                    
                    # Se lo status è uguale a 5, siamo nel proseguimento della
                    # routine relativa al time distribution di un giorno specifico
                    elif client["status"] == 5:
                        self.clients[i]["timetype"]=query_data
                        
                        # Inizializziamo un payload in cui inseriamo l'ID del microservice che
                        # vogliamo raggiungere
                        payload = {"ID" : "TimeAnalysis"}
                        
                        r_CD = requests.get(self.urlcatalog + "/getEndPoints",params=payload)
                        dictionary_CD = r_CD.json()

                        # Se non è online al momento, printiamolo e restituiamolo come risposta 
                        # a chiunque richieda i suoi servizi
                        if not dictionary_CD["exist"]:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                            return
                        
                        # Nel caso in cui fosse online, invece, salviamone l'url
                        url = dictionary_CD["url"]
                        
                        # Generiamo il payload contenente il tipo di timedistribution
                        # desiderato
                        payload = {'timetype': client["timetype"]}
                        
                        # Try-Except per prevenire errori dovuti al servizio
                        # inaspettatamente offline
                        try:
                            # Facciamo richiesta al servizio, dando come parametro
                            # il payload precedentemente generato
                            r2 = requests.get(url + "/TimeDistribution",params=payload)  
                            outList = r2.json()
                            
                            # Contiamo il numero di occurrence ottenuti in output
                            data = Counter(outList)
                            
                            # Inizializziamo una lista count vuota
                            count = []
    
                            # Inizializziamo una lista check, contenente tutte 
                            # le ore in cui il market è aperto
                            check = [str(num) for num in range(8,22)]
                            
                            # Iteriamo su tutti gli elementi di check
                            for el in check:
                                # Se l'ora dell'iterazione attuale non è contenuta
                                # in data.keys(), significa che non ci sono state
                                # transactions in quell'orario, quindi appendiamo
                                # uno zero
                                if el not in data.keys():
                                    count.append(0)
                                
                                # Altrimenti, appendiamo il numero di transactions
                                # per quello specifico orario
                                else:
                                    count.append(data[el])
                            
                            # Generiamo un Datafram pandas contenente le ore e il 
                            # numero di transactions relativo a tutte le ore
                            frame = {"time":check,"#transactions":count}
                            df = pd.DataFrame(frame,check)
    
                            # Generiamo il plot a barre
                            plot = df.plot.bar(x="time",rot=0)
                           
                            # Creiamo la figure relativa e mandiamola
                            # all'utente
                            fig = plot.get_figure()
                            buffer = BytesIO()
                            fig.savefig(buffer, format='png')
                            buffer.seek(0)
                            self.bot.sendPhoto(chat_id=client['chat_ID'], photo=buffer)
                        
                        # Nel caso in cui non fosse disponibile il servizio, 
                        # comunichiamolo al cliente
                        except:
                            self.bot.sendMessage(client['chat_ID'],text= 'Sorry, the service is not currently available, retry later! 🥺')
                        
                        # Settiamo in entrambi i casi del try-except lo status
                        # del cliente a zero, in modo che sia pronto ad effettuare
                        # una nuova richiesta
                        finally:
                            self.clients[i]["status"] = 0
                                                 
if __name__=="__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # E contattiamolo nuovamente per ottenere il token del BOT
    r2 = requests.get(urlcatalog + "/getTokenBOT")
    dictionary2 = r2.json() 
    
    # Creiamo l'oggetto BotMarket e startiamolo
    myBotMarket = BotMarket(dictionary2['token'],'mybotmarket',dictionary['broker'],dictionary['port'],dictionary['topic_employees_bot'])
    myBotMarket.start()

    while True :
        time.sleep(5)
