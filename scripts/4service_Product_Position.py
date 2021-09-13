import cherrypy
import time
import json
import requests

class REST_PP(object):
    exposed=True

    def __init__(self):
        self.urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]

    def GET(self,*uri,**params):
        
        # Salviamo la categoria e il prodotto passati come parametri in 
        # variabili locali
        if params!={}:
            category = params["category"]
            product = params["product"].lower()
        
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
            response = {"error" : "The TSAdaptor service is not currently available, retry later!"}
            print("The TSAdaptor service is not currently available, retry later!")
            return json.dumps(response,indent=4)
        
        # Nel caso in cui fosse disponibile, salviamo il suo URL, contattiamolo e
        # restituiamo i dati desiderati
        urlTSA = dictionary['url']+'/productPosition'
        payload = {"category":category,"product":product}

        r2 = requests.get(urlTSA,params=payload)
        response = r2.json()
        
        return json.dumps(response,indent=4)

if __name__=="__main__":
    # Apriamo il json che contiene l'url del catalog
    urlcatalog=json.load(open("catalogsettings.json"))["urlcatalog"]
    
    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tool.session.on':True        
            }    
        }   
    cherrypy.config.update({'server.socket_port':8040}) 
    cherrypy.tree.mount(REST_PP(),'/',conf)
    cherrypy.engine.start()
    
    # Ogni 5 minuti, inviamo una post request al catalog per comunicare che 
    # il servizio è online, con il relativo URL al quale è raggiungibile
    while True:
        payload = {"ID" : "ProdPosition", "url" : "http://127.0.0.1:8040"}
        payload = json.dumps(payload,indent=4)
        requests.post(urlcatalog + "/updateOnlineService",data=payload)
        time.sleep(300)
