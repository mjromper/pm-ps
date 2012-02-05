import json
import restlite
import PSHttpClient
import logging

logging.basicConfig(filename='pspm.log',format='[%(asctime)s] {%(levelname)s} - %(message)s',level=logging.DEBUG)


# The top-level events 
eventos = [{}]

# convert a Python object to resource

users = [{'username': 'kundan', 'name': 'Kundan Singh', 'email': 'kundan10@gmail.com'}, 
         {'username': 'alok'}]
users = restlite.bind(users)

# create an authenticated data model with one user and perform authentication for the resource

model = restlite.AuthModel()
model.register('kundan10@gmail.com', 'localhost', 'somepass')

data = '''
events
    id integer primary key
    offerId text not null
    type text not null
    userId text not null
'''
m = restlite.Model()
m.create(data)

@restlite.resource
def viewEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            itemId = str(dic['itemId'])
            timeDuration = str(dic['timeDuration'])
            logging.debug('POST (%s) event, data: %s', 'view', entity) 
            data = buildJSonEvent(entity,'view', timeDuration, None, None) 
            res = PSHttpClient.postEvent('view', itemId, data)
            logging.debug("view = %s,",str(res))
            return str(res)
        except: 
            logging.error("view err= %s,", 400)
            doPOSTexception(dic,itemId)           
            raise restlite.Status, '400 Error viewing event'
        
    
    return locals()


def doPOSTexception(dic, itemId):
    loc = dic['loc']
    latitude = loc['latitude']
    longitude = loc['longitude']
    logging.error("itemId (%s) does not exists in catalog", itemId)
    logging.error("Add items for location (%s,%s)", latitude, longitude)  
    

@restlite.resource
def purchaseEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            itemId = str(dic['itemId'])
            logging.debug('POST (%s) event, data: %s', 'purchase', entity) 
            data = buildJSonEvent(entity, 'purchase', None, None, None)
            res = PSHttpClient.postEvent('purchase', itemId, data)
            logging.debug("purchase = %s,",str(res))
            return str(res)
        except: 
            logging.error("purchase err= %s,", 400)
            doPOSTexception(dic,itemId)   
            raise restlite.Status, '400 Error purchasing event'
    return locals()

@restlite.resource
def clickEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            logging.debug("Location %,", 400)
            logging.debug('POST (%s) event, data: %s', 'click', entity)
            itemId = str(dic['itemId'])
            recommId = str(dic['recommId'])
            data = buildJSonEvent(entity, 'click', None, recommId,None)
            res = PSHttpClient.postEvent('click', itemId, data)  
            logging.debug("click = %s,",str(res))
            return str(res)
        except: 
            logging.error("click err= %s,", 400)
            doPOSTexception(dic,itemId)   
            raise restlite.Status, '400 Error click event'
    return locals()

@restlite.resource
def rateEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            logging.debug('POST (%s) event, data: %s', 'rate', entity)
            itemId = str(dic['itemId'])
            rating = str(dic['rating'])
            data = buildJSonEvent(entity, 'rate', None, None, rating)
            res = PSHttpClient.postEvent('rate', itemId, data)
            logging.debug("rate = %s,",str(res))
            return str(res)
        except: 
            logging.error("rate err= %s,", 400)
            doPOSTexception(dic,itemId)   
            raise restlite.Status, '400 Error click event'
    return locals()

@restlite.resource
def top():
    def PUT(request, entity):
        try:
            order = entity
            res = PSHttpClient.getTopRecomm(order)
            logging.debug("top found = OK")
            return str(res)
        except: raise restlite.Status, '400 Error top recomms'
    return locals()

@restlite.resource
def similar():
    def PUT(request, entity):
        try:
            itemId = entity
            res = PSHttpClient.getRecommSimilar(itemId, 20)
            logging.debug("similar = OK")
            return str(res)
        except: raise restlite.Status, '400 Error top recomms'
    return locals()

@restlite.resource
def byUser():
    def PUT(request, entity):
        try:
            msisdn = entity
            print(msisdn)
            res = PSHttpClient.getRecommByUser(msisdn, 20);
            logging.debug("byUser = OK")
            return str(res)
        except: raise restlite.Status, '400 Error top recomms'
    return locals()


@restlite.resource
def event():
    def GET(request):
        event = request['PATH_INFO'][1:] if request['PATH_INFO'] else ''
        eventType=['view', 'purchase', 'click', 'rate']
        if not event:
            rows = m.sql('SELECT * FROM events')
            result = fillResults(rows)
              
        elif event in eventType:
            rows = m.sql('SELECT * FROM events WHERE type=?', (event,))
            result = fillResults(rows)
                            
        else: 
            rows = m.sql('SELECT * FROM events WHERE id=?', (event,))   
            if rows is None: raise restlite.Status, '404 Event Not Found'
            
            result = fillResults(rows) 
        
        salida = {'totalResults':len(result), 'data': result} if result is not None else ''   
        
        logging.debug("events found = %s",str(len(result))) 
        return request.response(('events', salida))
    
    return locals()

@restlite.resource
def eventByUser():
    def GET(request):
        userId = request['PATH_INFO'][1:] if request['PATH_INFO'] else ''
        if not userId:
            rows = m.sql('SELECT * FROM events')
            result = fillResults(rows)
                                         
        else: 
            vars = userId.split('/')  
            if len(vars)==2 :              
                rows = m.sql('SELECT * FROM events WHERE userId=? AND type=?', (vars[0], vars[1]))  
            else:
                rows = m.sql('SELECT * FROM events WHERE userId=?', (vars[0],))   
                  
            if rows is None: raise restlite.Status, '404 Event Not Found'
            
            result = fillResults(rows)
        
        salida = {'totalResults':len(result), 'data': result} if result is not None else ''
        logging.debug("events found = %s", str(len(result)))    
        return request.response(('events', salida))
        
    return locals()

@restlite.resource
def eventByItem():
    def GET(request):
        offerId = request['PATH_INFO'][1:] if request['PATH_INFO'] else ''
        if not offerId:
            rows = m.sql('SELECT * FROM events')
            result = fillResults(rows)
                                         
        else: 
            vars = offerId.split('/')  
            if len(vars)==2 :              
                rows = m.sql('SELECT * FROM events WHERE offerId=? AND type=?', (vars[0], vars[1]))  
            else:
                rows = m.sql('SELECT * FROM events WHERE offerId=?', (vars[0],))   
                  
            if rows is None: raise restlite.Status, '404 Event Not Found'
            
            result = fillResults(rows)
        
        salida = {'totalResults':len(result), 'data': result} if result is not None else ''
        logging.debug("events found = %s", str(len(result)))    
        return request.response(('events', salida))
        
    return locals()


# Insert in database the new event and build JSON for PS Request
def buildJSonEvent(entity, eventType, duration, recommId, rating):
       
    from time import gmtime, strftime
    date = strftime("%Y-%m-%dT%H:%M:%S", gmtime())+'+00:00'
    
    dic = json.loads(entity)
    userId = str(dic['userId'])
    itemId = str(dic['itemId'])
    type = eventType
    index = m.sql('INSERT INTO events VALUES (NULL, ?,?,?)', (itemId, type, userId))
    lastRowId = str(index.lastrowid)
    
    jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android"}
     
    if duration is not None: 
        jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android", "timeDuration": duration} 
    elif recommId is not None: 
        jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android", "recommId": recommId} 
    elif rating is not None: 
        jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android", "rating": rating, "ratingScale": "5"}   
        
    data = json.dumps(jsonData)
    logging.debug("Data inserted: %s",data)
    
    return data


# Fill the list of events obtained from database 
def fillResults(rows):
    result = []
    for row in rows:
        result.append({"eventId": row[0], "offerId":row[1], "type":row[2] ,"userId": row[3]})
    return result


# all the routes
routes = [
    (r'GET,PUT,POST /(?P<type>((xml)|(plain)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=text/%(type)s'),
    (r'GET,PUT,POST /(?P<type>((json)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=application/%(type)s'),
    (r'GET /events/user', eventByUser),
    (r'GET /events/item', eventByItem),
    (r'GET /events', event),
    (r'GET /recomms/top\?order=(?P<order>.*)', 'PUT /recomms/top', 'CONTENT_TYPE=text/plain', 'BODY=%(order)s', top),
    (r'GET /recomms/similar\?itemId=(?P<itemId>.*)', 'PUT /recomms/similar', 'CONTENT_TYPE=text/plain', 'BODY=%(itemId)s', similar),
    (r'GET /recomms/byUser\?msisdn=(?P<msisdn>.*)', 'PUT /recomms/byUser', 'CONTENT_TYPE=text/plain', 'BODY=%(msisdn)s', byUser),
    (r'POST /event/view', 'POST /event/view', 'CONTENT_TYPE=text/plain', viewEvent),
    (r'POST /event/purchase', 'POST /event/purchase', 'CONTENT_TYPE=text/plain', purchaseEvent),
    (r'POST /event/rate', 'POST /event/rate', 'CONTENT_TYPE=text/plain', rateEvent),
    (r'POST /event/click', 'POST /event/click', 'CONTENT_TYPE=text/plain', clickEvent)
    

]        

# launch the server on port 9000
    
if __name__ == '__main__':
    import sys
    from wsgiref.simple_server import make_server
    
    httpd = make_server('', 9000, restlite.router(routes))
            
    try: httpd.serve_forever()
    except KeyboardInterrupt: pass
