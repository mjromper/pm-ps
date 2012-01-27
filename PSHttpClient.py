'''
Created on Jan 25, 2012

@author: MRomero2
'''
import urllib2
import httplib

class httpClient(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
       '''
    def _launchRequest(self, request):
        try: 
            response = urllib2.urlopen(request)
            return response.code, response.read()
        except urllib2.HTTPError, e:
            return('HTTPError = ' + str(e.code))
        except urllib2.URLError, e:
            return('URLError = ' + str(e.reason))
        except httplib.HTTPException, e:
            return('HTTPException')
        except Exception:
            import traceback
            return('generic exception: ' + traceback.format_exc())
            

headers = {"Content-type": "application/json","Authorization": "Basic YWRtaW46YWRtaW4=","Accept": "text/plain", "Accept-Encoding": "UTF-8"}
url = "http://176.34.228.163/ps/o2prioritymoments/"
client = httpClient()

def getTopRecomm(number):
    host = url+'recomms/top?type=PriorityMoments&numRecos='+str(number)
    req = urllib2.Request(host, None, headers)
    response = client._launchRequest(req)
    return response

def getRecommByUser(msisdn, number):
    host = url+'recomms/byUser?type=PriorityMoments&userRecomm='+msisdn+'&numRecos='+str(number)
    print(host)
    req = urllib2.Request(host, None, headers)
    response = client._launchRequest(req)
    return response       
    

def postEvent(eventType, itemId, jsonString):
    host = url+'items/'+itemId+'/events/'+eventType
    req = urllib2.Request(host, jsonString, headers)
    response = client._launchRequest(req)
    return response






