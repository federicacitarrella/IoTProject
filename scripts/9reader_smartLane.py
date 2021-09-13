import paho.mqtt.client as PahoMQTT
import thingspeak
import time
import random
import numpy as np
import requests
import json
import datetime

class RFID_lane(object):

    def __init__(self, clientID, broker, port):
        self.clientID= clientID 
        self.broker=broker
        self.port=port
        self._paho_mqtt = PahoMQTT.Client(self.clientID,True)
        self._paho_mqtt.on_connect = self.myOnConnect

    def start(self):
        self._paho_mqtt.connect(self.broker, self.port) 
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.loop_stop() 
        self._paho_mqtt.disconnect()

    def myOnConnect (self, paho_mqtt, userdata, flags, rc):
        print ("Connected to %s with result code: %d" % (self.broker, rc))

    def publish(self, topic, message):
        self._paho_mqtt.publish(topic, message, 2)

if __name__ == "__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    # Contattiamolo chiedendo gli endpoints di Mosquitto (broker, port, topics)
    r = requests.get(urlcatalog + "/MosquittoEndPoints")
    dictionary = r.json()
    
    # Contattiamolo per richiedere la lista delle transactions fittizie
    r2 = requests.get(urlcatalog + "/getTransactions")
    dictionary2 = r2.json()
    
    # E contattiamolo nuovamente per ottenere l'url del servizio dedicato agli 
    # sconti
    
    # Inizializziamo un payload in cui inseriamo l'ID del microservice che
    # vogliamo raggiungere
    payload = {"ID" : "CheckDiscounts"}

    r_CD = requests.get(urlcatalog + "/getEndPoints",params=payload)
    dictionary_CD = r_CD.json()

    # Se non è online al momento, printiamolo e restituiamolo come risposta 
    # a chiunque richieda i suoi servizi
    if not dictionary_CD["exist"]:
        print("The CheckDiscounts service is not currently available, retry later!")
        exit()

    url = dictionary_CD["url"]
    
    # Salviamo nelle variabili ciò che abbiamo ottenuto dalle get requests
    transactions = dictionary2["transactions"]
    topic = dictionary["topic_reader_lane"]
    
    # Inizializziamo il nostro oggetto RFID_lane e startiamolo
    pubID = "SmartLaneReader"
    mySmartLane = RFID_lane(pubID, dictionary['broker'], dictionary['port'])
    mySmartLane.start()
    
    # Contiamo il numero di transactions a nostra disposizione
    num = len(transactions)
    
    # Generiamo, seguendo un andamento gaussiano, una lista di orari fittizi
    # centrati intorno alle 12 e alle 19 (orari di punta medi dei supermarket) con
    # una deviazione standard di 1.5, in modo da associare ad ogni transaction 
    # un orario di esecuzione
    timeStamps1 = np.random.normal(int(11),float(1.5),int(np.round(num/2))).tolist()
    timeStamps2 = np.random.normal(int(18),float(1.5),int(num-np.round(num/2))).tolist()
    
    # Otteniamo così una lista di orari (al momento float) della stessa lunghezza
    # delle transactions
    timeStamps = timeStamps1+timeStamps2
    
    # Iterando su tale lista, convertiamo i float in orari nel formato HH:MM:SS
    for i,hour in enumerate(timeStamps):
        temp = datetime.timedelta(hours=hour)
        timeStamps[i] = str(temp).split('.')[0]
     
    # Inizializziamo una lista dei giorni della settimana
    days = ["sun","mon","tue","wed","thu","fri","sat"]
    
    # Al fine di simulare lo scorrere del tempo in maniera "più rapida" e conveniente,
    # associamo ad ogni giorno della settimana un corrispondente lasso di tempo "reale".
    # Come è possibile notare, i giorni del weekend presentano un lasso di tempo 
    # più esteso, in modo da registrarvi più transazioni e, di conseguenza,
    # simulare una maggior affluenza in tali giorni, come di fatto accade
    # nel mondo reale
    minutes = [3,1,1,1,1,2,2]
    
    # Trasformiamo la lista minutes in una cumulata, in modo da gestire più
    # comodamente il tutto
    minutes = np.cumsum(minutes)
    
    # Settiamo il timestamp del momento in cui startiamo tutto; d'ora in avanti
    # calcoleremo quanto tempo è passato per stabilire quando cambiare giorno 
    # della settimana
    timeStart = time.time()

    # Inizializzamo una variabile previousDay, che al momento è vuota
    previousDay = ""

    # Iteriamo contemporaneamente su timestamps e transactions
    for hour,transaction in zip(timeStamps,transactions):
        # Inizializziamo un contatore totale, che verrà incrementato volta per
        # volta fino a calcolare il prezzo totale degli articoli contenuti
        # all'interno delle singole transactions
        total = 0
        
        # Inizializziamo un payload in cui inseriamo l'ID del microservice che
        # vogliamo raggiungere
        payload = {"ID" : "TSAdaptor"}
        
        # Tramite get request contattiamo il catalog, chiedendo se il microservice
        # desiderato è online e, in tal caso, ricevendo il suo URL
        r3 = requests.get(urlcatalog + "/getEndPoints", params=payload)
        dictionary3 = r3.json()
        
        # Se non è online al momento, printiamolo e restituiamolo come risposta 
        # a chiunque richieda i suoi servizi
        if not dictionary3["exist"]:
            response = {"error" : "The TSAdaptor service is not currently available, retry later!"}
            print("The TSAdaptor service is not currently available, retry later!")
            break
        
        # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
        # chiediamo i prezzi dei singoli prodotti della transaction attuale
        urlTSA = dictionary3['url']+'/getPrices'
        payload4 = {"list":transaction}
        
        r4 = requests.get(urlTSA,params=payload4)
        response4 = r4.json()
        
        # Calcoliamo il timeStop come la differenza tra il tempo attuale e quello
        # di inizio, in modo da capire quanto tempo è passato
        timeStop = int(time.time() - timeStart)
        
        # Convertiamo il tutto in minuti dividendo per 60. Se il valore è maggiore 
        # al tempo totale stabilito per l'intera settimana, reinizializziamo il 
        # timeStart ricominciando di fatto la settimana
        if timeStop/60>minutes[-1]:
            timeStart = time.time()
            timeStop = int(time.time() - timeStart)
            
        # Iteriamo ora su days e minutes
        for day,minute in zip(days,minutes):
            # Salviamo in actual day il giorno attuale
            actualDay = day
            
            # Se il tempo passato è inferiore al tempo previsto per la giornata specifica,
            # salviamo la transaction come avvenuta nel giorno specifico. Ad esempio, se il 
            # tempo trascorso è di 2 minuti, la transaction sarà effettuata di domenica,
            # dal momento in cui tale giorno ha una durata di 3 minuti.
            # Se il timestop fosse invece di 6 minuti, la transazione sarebbe attribuita a
            # mercoledì
            if timeStop/60<=minute:
                message = {"transaction" : transaction, "timeStamp" : day+"/"+str(hour), "total" : 0}
                break
        
        # Se il giorno precedente è diverso dal giorno attuale (quindi abbiamo
        # cambiato giorno con lo scorrere del tempo), richiediamo tramite
        # get request le offerte del giorno attuale. In questo modo, ogni giorno 
        # avremo nuove offerte aggiornate in base alle transactions del giorno 
        # precedente e saremo in grado di calcolare il totale in maniera corretta,
        # tenendo conto dei prezzi scontati corretti
        if previousDay != actualDay:
            # Richiediamo le offerte sui singoli prodotti
            payload = {'offertype': "SingleProducts"}
            r2 = requests.get(url + "/CheckDiscounts",params=payload) 
            dictionary2 = r2.json()
            discounted = []
            
            # Ottenendo il loro prezzo scontato
            for el in dictionary2["products"]:
                parts = el.split("/")
                discounted.append({"product":parts[0],"discPrice":parts[2]})
            
            # E facciamo lo stesso sui prodotti contenuti nelle association rules
            payload = {'offertype': "CoupledProducts"}
            r3 = requests.get(url + "/CheckDiscounts",params=payload)  
            dictionary3 = r3.json()
            discountedCoupled = dictionary3["products"]
            
        # Iteriamo sui singoli prodotti della transaction attuale, recuperandone anche
        # l'indice
        for index,el in enumerate(transaction):
            # Settiamo una flag restart a False
            restart = False
            
            # Facciamo un check su antecedenti/conseguenti. Iteriamo tutti gli sconti
            # di coppia che ci sono 
            for couple in discountedCoupled:
                # Se il prodotto attualmente iterato è presente come conseguente in
                # un'offerta
                if el in couple["consequents"]:
                    # E all'interno della transaction attuale sono contenuti tutti
                    # gli antecedenti e tutti i conseguenti
                    if all(elem in transaction for elem in couple["antecedents"]+couple["consequents"]):
                        # Incrementiamo il totale del perzzo del prodotto attuale, 
                        # scontato del 50%
                        total += float(response4["prices"][index])/2
                        
                        # Settiamo restart uguale a True
                        restart = True
                        break
            # Se il restart è uguale a true, significa che abbiamo già conteggiato
            # il prodotto attuale nel totale della transaction, quindi possiamo 
            # proseguire alla successiva iterazione tramite continue
            if restart:
                continue
            
            # Se il restar è ancora False, controlliamo se il prodotto è presente
            # negli sconti singoli; in tal caso incrementiamo il totale del suo prezzo
            # scontato
            for disc in discounted:
                if disc["product"]==el:
                    total += float(disc["discPrice"])
                    restart = True
                    break
            
            # Check sul restart, come prima
            if restart:
                continue
            
            # Se il prodotto non è presente in nessuna delle liste dei prodotti
            # scontati, conteggiamo il suo prezzo pieno nel totale e passiamo 
            # al prodotto successivo
            total += float(response4["prices"][index])

        # Una volta conteggiati i prezzi di tutti gli articoli e calcolato il totale,
        # salviamolo all'interno del messaggio
        message["total"] = format(total,".2f")
        
        # Pubblichiamo il messaggio sul topic della reader lane
        mySmartLane.publish(topic,json.dumps(message,indent=4))
        
        # Settiamo il giorno precedente uguale al giorno attuale, in modo da tener
        # conto dello scorrere del tempo
        previousDay = actualDay
        
        time.sleep(30)

