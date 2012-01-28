import json
import restlite
import PSHttpClient

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
def private():
    def GET(request):
        global model
        model.login(request)
        return request.response(('path', request['PATH_INFO']))
    return locals()

@restlite.resource
def viewEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            itemId = str(dic['itemId'])
            timeDuration = str(dic['timeDuration'])
            data = buildJSonEvent(entity,'view', timeDuration, None) 
            print(data)             
            res = PSHttpClient.postEvent('view', itemId, data)
            return str(res)
        except: raise restlite.Status, '400 Error viewing event'
    return locals()

@restlite.resource
def purchaseEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            itemId = str(dic['itemId'])
            data = buildJSonEvent(entity, 'purchase')
            res = PSHttpClient.postEvent('purchase', itemId, data)
            return str(res)
        except: raise restlite.Status, '400 Error purchasing event'
    return locals()

@restlite.resource
def clickEvent():
    def POST(request, entity):
        try:
            dic = json.loads(entity)
            itemId = str(dic['itemId'])
            recommId = str(dic['recommId'])
            data = buildJSonEvent(entity, 'click', None, recommId)
            res = PSHttpClient.postEvent('click', itemId, data)
            return str(res)
        except: raise restlite.Status, '400 Error click event'
    return locals()

@restlite.resource
def top():
    def PUT(request, entity):
        try:
            num = entity
            print(num)
            res = PSHttpClient.getTopRecomm(num)
            return str(res)
        except: raise restlite.Status, '400 Error top recomms'
    return locals()


@restlite.resource
def event():
    def GET(request):
        event = request['PATH_INFO'][1:] if request['PATH_INFO'] else ''
        eventType=['view', 'purchase', 'click']
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
            rows = m.sql('SELECT * FROM events WHERE userId=?', (userId,))   
            if rows is None: raise restlite.Status, '404 Event Not Found'
            
            result = fillResults(rows)
        
        salida = {'totalResults':len(result), 'data': result} if result is not None else ''    
        return request.response(('events', salida))
        
    return locals()


# Insert in database the new event and build JSON for PS Request
def buildJSonEvent(entity, eventType, duration=None, recommId=None):
    dic = json.loads(entity)
    userId = str(dic['userId'])
    itemId = str(dic['itemId'])
    index = m.sql('INSERT INTO events VALUES (NULL, ?,?,?)', (itemId, eventType, userId))
    lastRowId = str(index.lastrowid)
    from time import gmtime, strftime
    date = strftime("%Y-%m-%dT%H:%M:%S", gmtime())+'+00:00'
    jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android"}
    if duration is not u"" or None: jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android", "timeDuration": duration} 
    
    if recommId is not u"" or None: jsonData = {"eventId": lastRowId , "timestamp": date, "userId": userId, "channel": "android", "recommId": recommId} 
   
    data = json.dumps(jsonData)
    return data


# Fill the list of events obtained from database 
def fillResults(rows):
    result = []
    for row in rows:
        result.append({"eventId": row[0], "itemId":row[1], "type":row[2] ,"userId": row[3]})
    return result


# all the routes
routes = [
    (r'GET,PUT,POST /(?P<type>((xml)|(plain)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=text/%(type)s'),
    (r'GET,PUT,POST /(?P<type>((json)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=application/%(type)s'),
    (r'GET /events/user', eventByUser),
    (r'GET /events', event),
    (r'GET /top\?num=(?P<num>.*)', 'PUT /top', 'CONTENT_TYPE=text/plain', 'BODY=%(num)s', top),
    (r'POST /event/view', 'POST /event/view', 'CONTENT_TYPE=text/plain', viewEvent),
    (r'POST /event/purchase', 'POST /event/purchase', 'CONTENT_TYPE=text/plain', purchaseEvent),
    (r'POST /event/click', 'POST /event/click', 'CONTENT_TYPE=text/plain', clickEvent)
    

]        

# launch the server on port 9000
    
if __name__ == '__main__':
    import sys
    from wsgiref.simple_server import make_server
    
    httpd = make_server('', 9000, restlite.router(routes))
            
    try: httpd.serve_forever()
    except KeyboardInterrupt: pass
