import json
import time

class CatalogManager():
    def __init__(self,filename):
        self.fileName=filename
        self.catalogDict={}

        #Carichiamo il file JSON del catalog e salviamolo su un dizionario
        fp=open(self.fileName)
        self.catalogDict=json.load(fp) 
        self.onlineServices = self.catalogDict["onlineServices"]
    
    # Restituiamo gli endpoints (URL) del microservice passato come parametro
    def getEndPoints(self,ID):
        result = {"url" : None, "exist" : False}
        for service in self.catalogDict["onlineServices"]:
            if service["ID"] == ID:
                result["url"] = service["url"]  
                result["exist"] = True
        
        return json.dumps(result,indent=4)
    
    # Restituiamo broker, port e topics MQTT
    def MosquittoEndPoints(self):
        result= {
            "broker": self.catalogDict['Mosquitto']['broker'],
            "port":self.catalogDict['Mosquitto']['port'],
            "topic_reader_store":self.catalogDict['Mosquitto']['topic_reader_store'],
            "topic_reader_lane":self.catalogDict['Mosquitto']['topic_reader_lane'],
            "topic_thingspeak_updated":self.catalogDict['Mosquitto']['topic_thingspeak_updated'],
            "topic_employees_bot":self.catalogDict['Mosquitto']['topic_employees_bot'],
            "topic_reader_store_restock":self.catalogDict['Mosquitto']['topic_reader_store_restock']
            }  
            
        return json.dumps(result,indent=4)
    
    # Restituiamo l'User API di Thingspeak
    def TSUserAPI(self):
        result= {
            "UserAPI" : self.catalogDict["ThingSpeak"]["UserAPI"]
            }

        return json.dumps(result,indent=4)
    
    # Restituiamo la lista di transactions fittizie salvate nel catalog
    def getTransactions(self):
        result= {
            "transactions":self.catalogDict['transactions']
            }  
            
        return json.dumps(result,indent=4)
    
    # Restituiamo il token del BOT
    def getTokenBot(self):
        result= {
            "token" : self.catalogDict['Telegram']["token"]
            }  
            
        return json.dumps(result,indent=4)
    
    # Restituiamo gli employees salvati nel catalog
    def GetEmployees(self):
        result= {"employees": self.catalogDict["employees"]["users"]}  
            
        return json.dumps(result,indent=4)
    
    # Effettuiamo un check se la pwd inserita per accedere come employee 
    # è corretta o meno
    def CheckPassword(self, pw):
        if pw == self.catalogDict["employees"]["password"]:
            result= {"Success": True}
        else:
            result= {"Success": False}
            
        return json.dumps(result,indent=4)
    
    
    # Dumpiamo nel catalog gli utenti che si sono autenticati come
    # employees, in modo da non dover chieder loro ogni volta una nuova
    # autenticazione
    def updateEmployees(self,employees):
        self.catalogDict["employees"]["lastUpdate"] = time.time()
        self.catalogDict["employees"]["users"] = employees
        json.dump(self.catalogDict,open("catalog.json","w"),indent=4)
        return 
    
    # Dopo aver fornito ID e url, un servizio viene iscritto al catalog come
    # online Service, quindi raggiungibile 
    def updateOnlineService(self,ID, url):
        #Carichiamo il file JSON del catalog e salviamolo su un dizionario
        fp=open(self.fileName)
        self.catalogDict=json.load(fp) 
        self.onlineServices = self.catalogDict["onlineServices"]
        
        # Calcoliamo il momento in cui il servizio si è iscritto l'ultima volta
        timeStamp = time.time()
        
        # Generiamo un template contenente ID, url e timestamp del servizio
        # che si vuole iscrivere
        onlineService = {'ID':ID,'url':url,'timeStamp':timeStamp}
        
        # Settiamo una flag False
        esiste = False
        
        # Iteriamo sui servizi online già presenti nel catalog 
        for idx,service in enumerate(self.onlineServices):
            # Se tale servizio è già presente, aggiorniamolo con il nuovo
            # timestamp, in modo che non venga eliminato per inattività dopo 
            # tot tempo
            if service["ID"] == onlineService["ID"]:
                esiste = True
                self.onlineServices[idx] = onlineService
                self.catalogDict["onlineServices"] = self.onlineServices
                json.dump(self.catalogDict,open("catalog.json","w"),indent=4)
                return
        
        # Se non è già presente nel catalog, inseriamolo
        if not esiste:
            self.onlineServices.append(onlineService)
            self.catalogDict["onlineServices"] = self.onlineServices
            json.dump(self.catalogDict,open("catalog.json","w"),indent=4)
            return
        
    # Rimuove gli online services che non si sono registrati entro 10 minuti
    # in modo da renderli irraggiungibili
    def removeServices(self):
        #Carichiamo il file JSON del catalog e salviamolo su un dizionario
        fp=open(self.fileName)
        self.catalogDict=json.load(fp) 
        self.onlineServices = self.catalogDict["onlineServices"]
        
        for service in self.onlineServices:
            # Se son passati più di 10 minuti, rimuoviamo il servizio dalla lista
            if time.time()-service["timeStamp"]>600:
                self.onlineServices.remove(service)
                self.catalogDict["onlineServices"] = self.onlineServices
                json.dump(self.catalogDict,open("catalog.json","w"),indent=4)
                

if __name__=="__main__":
    prova=CatalogManager("catalog.json")