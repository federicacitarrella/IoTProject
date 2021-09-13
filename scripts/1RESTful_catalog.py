import cherrypy
import time
import json
from CatalogManager import *

class RESTCatalog(object):
    exposed=True

    def __init__(self):
        self.manager = CatalogManager("catalog.json")

    def GET(self,*uri,**params):

        uri = uri[0]
        
        # Salviamo i parametri passati in input in una lista, che sfrutteremo 
        # da qui in avanti
        if params!={}:
           valuesList = [params[key] for key in params.keys()]
           lista = []
           for el in valuesList:
                lista.append(el)
        
        # Restituiamo gli endpoints (URL) del microservice passato come parametro
        if uri == "getEndPoints":
            response = self.manager.getEndPoints(lista[0])
            return response
        
        # Restituiamo broker, port e topics MQTT
        elif uri == "MosquittoEndPoints":
            response = self.manager.MosquittoEndPoints()
            return response
        
        # Restituiamo l'User API di Thingspeak
        elif uri == "TSUserAPI":
            response = self.manager.TSUserAPI()
            return response
        
        # Restituiamo la lista di transactions fittizie salvate nel catalog
        elif uri == "getTransactions":
            response = self.manager.getTransactions()
            return response
        
        # Restituiamo il token del BOT
        elif uri == "getTokenBOT":
            response = self.manager.getTokenBot()
            return response
        
        # Restituiamo gli employees salvati nel catalog
        elif uri == "GetEmployees":
            response = self.manager.GetEmployees()
            return response
        
        # Effettuiamo un check se la pwd inserita per accedere come employee 
        # è corretta o meno
        elif uri == "CheckPassword":
            response = self.manager.CheckPassword(lista[0])
            return response
        
    def POST(self,*uri,**params):
        uri = uri[0]
        
        # Inseriamo nel catalog gli utenti che si sono autenticati come employees
        if uri == "updateEmployees":
            dictionary = cherrypy.request.body.read()
            dictionary = json.loads(dictionary)
            self.manager.updateEmployees(dictionary['employees'])
            return 
        
        # Permettiamo ai services di iscriversi al catalog, comunicando che sono
        # up and running
        if uri == "updateOnlineService":
            dictionary = cherrypy.request.body.read()
            dictionary = json.loads(dictionary)
            self.manager.updateOnlineService(dictionary['ID'],dictionary['url'])
            return 

if __name__=="__main__":
    
    conf={
        '/':{
            'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
            'tool.session.on':True        
            }    
        }   
    cherrypy.config.update({'server.socket_port':8080}) 
    cherrypy.tree.mount(RESTCatalog(),'/',conf)
    cherrypy.engine.start()
    
    manager = CatalogManager("catalog.json")
    
    while True:
        # Facciamo un check periodico sui servizi attivi e rimuoviamo quelli che 
        # non si registrano regolarmente 
        manager.removeServices()
        time.sleep(30)