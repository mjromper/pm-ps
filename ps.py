import os, thread, md5, json
import restlite
import PSHttpClient

# The top-level directory for all file requests

directory = '.'
eventos = [{}]

# The resource to get or set the top-level directory

@restlite.resource
def config():
    def GET(request):
        global directory
        return request.response(('config', ('directory', directory)))
    def PUT(request, entity):
        global directory
        directory = str(entity)
    return locals()

# The resource to list all the file information under path relative to the top-level directory

@restlite.resource
def files():
    def GET(request):
        global directory
        if '..' in request['PATH_INFO']: raise restlite.Status, '400 Invalid Path'
        path = os.path.join(directory, request['PATH_INFO'][1:] if request['PATH_INFO'] else '')
        try:
            files = [(name, os.path.join(path, name), request['PATH_INFO'] + '/' + name) for name in os.listdir(path)]
        except: raise restlite.Status, '404 Not Found'
        def desc(name, path, url):
            if os.path.isfile(path):
                return ('file', (('name', name), ('url', '/file'+url), ('size', os.path.getsize(path)), ('mtime', int(os.path.getmtime(path)))))
            elif os.path.isdir(path):
                return ('dir', (('name', name), ('url', '/files'+url)))
        files = [desc(*file) for file in files]
        return request.response(('files', files))
    return locals()

# download a given file from the path under top-level directory

def file(env, start_response):
    global directory
    path = os.path.join(directory, env['PATH_INFO'][1:] if env['PATH_INFO'] else '')
    if not os.path.isfile(path): raise restlite.Status, '404 Not Found'
    start_response('200 OK', [('Content-Type', 'application/octet-stream')])
    try:
        with open(path, 'rb') as f: result = f.read()
    except: raise restlite.Status, '400 Error Reading File'
    return [result]




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
        dic = json.loads(entity)
        itemId = str(dic['itemId'])
        timeDuration = str(dic['timeDuration'])
        data = buildJSonEvent(entity,'view', timeDuration, None) 
        print(data)             
        res = PSHttpClient.postEvent('view', itemId, data)
        return str(res)
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
        dic = json.loads(entity)
        itemId = str(dic['itemId'])
        recommId = str(dic['recommId'])
        data = buildJSonEvent(entity, 'click', None, recommId)
        res = PSHttpClient.postEvent('click', itemId, data)
        return str(res)
    return locals()


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

           
@restlite.resource
def top():
    def PUT(request, entity):
        num = entity
        print(num)
        res = PSHttpClient.getTopRecomm(num)
        return str(res)
    return locals()


@restlite.resource
def evento():
    def GET(request):
        evento = request['PATH_INFO'][1:] if request['PATH_INFO'] else ''
        eventType=['view', 'purchase', 'click']
        if not evento:
            rows = m.sql('SELECT * FROM events')
            result = []
            for row in rows:
                result.append({"eventId": row[0], "itemId":row[1], "type":row[2] ,"userId": row[3]})
            
        
        elif evento in eventType:
            rows = m.sql('SELECT * FROM events WHERE type=?', (evento,))
            result = []
            for row in rows:
                result.append({"eventId": row[0], "itemId":row[1], "type":row[2] ,"userId": row[3]})
                            
        else: 
            rows = m.sql1('SELECT * FROM events WHERE id=?', (evento,))   
            if rows is None: 
                raise restlite.Status, '404 Event Not Found'
            
            result = {"eventId": rows[0], "itemId":rows[1], "type":rows[2] ,"userId": rows[3]}  
        
        salida = [result] if result is not None else []    
        return request.response(('events', salida))
    
    return locals()

# all the routes
routes = [
    (r'GET,PUT,POST /(?P<type>((xml)|(plain)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=text/%(type)s'),
    (r'GET,PUT,POST /(?P<type>((json)))/(?P<path>.*)', 'GET,PUT,POST /%(path)s', 'ACCEPT=application/%(type)s'),
    (r'POST /event/view', 'POST /event/view', 'CONTENT_TYPE=text/plain', viewEvent),
    (r'POST /event/purchase', 'POST /event/purchase', 'CONTENT_TYPE=text/plain', purchaseEvent),
    (r'POST /event/click', 'POST /event/click', 'CONTENT_TYPE=text/plain', clickEvent),
    (r'GET /top\?num=(?P<num>.*)', 'PUT /top', 'CONTENT_TYPE=text/plain', 'BODY=%(num)s', top),
    (r'GET /events', evento)
]        

# launch the server on port 9000
    
if __name__ == '__main__':
    import sys
    from wsgiref.simple_server import make_server
    
    httpd = make_server('', 9000, restlite.router(routes))
    
    # if unit test is desired, perform unit testing
    if len(sys.argv) > 1 and sys.argv[1] == '--unittest':
        import urllib2, cookielib
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        top_level_url = "localhost:9000"
        password_mgr.add_password(None, top_level_url, "kundan10@gmail.com", "somepass")
        cj = cookielib.CookieJar()
        urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(password_mgr), urllib2.HTTPCookieProcessor(cj)))
        
        def urlopen(url, prefix="http://localhost:9000"):
            try: return urllib2.urlopen(prefix + url).read()
            except: return sys.exc_info()[1]
            
        def test():
            print urlopen('/config')
            print urlopen('/config?directory=..')
            print urlopen('/config')
            print urlopen('/xml/files')
            print urlopen('/xml/files/restlite')
            print urlopen('/json/files')
            print urlopen('/json/files/restlite')
            print '\n'.join(urlopen('/file/restlite/restlite.py').split('\n')[:6])
            print urlopen('/json/users')
            print urlopen('/json/users/0')
            print urlopen('/json/users/1/username')
            print urlopen('/json/private/something/is/here')
            print urlopen('/json/private/otherthing/is/also/here')
            
        thread.start_new_thread(test, ())
    
    try: httpd.serve_forever()
    except KeyboardInterrupt: pass
