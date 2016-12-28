"""
Module Name:  d2cMsgSender.py
Project:      IoTHubRestSample
Copyright (c) Microsoft Corporation.

Using [Send device-to-cloud message](https://msdn.microsoft.com/en-US/library/azure/mt590784.aspx) API to send device-to-cloud message from the simulated device application to IoT Hub.

This source is subject to the Microsoft Public License.
See http://www.microsoft.com/en-us/openness/licenses.aspx#MPL
All other rights reserved.

THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED 
WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
"""

import base64
import hmac
import hashlib
import time
import requests
import urllib
import sys

sys.path.append('..')
import pydocutils

class D2CMsgSender:
    
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'

    # document db info
    HOST = '{{cookiecutter.documentdb_serviceUri}}'
    KEY = '{{cookiecutter.documentdb_key}}'
    DATABASE = '{{cookiecutter.documentdb_database}}'
    COLLECTION = '{{cookiecutter.documentdb_collection}}'
    #
    
    def __init__(self, connectionString=None):
        if connectionString != None:
            iotHost, keyName, keyValue = [sub[sub.index('=') + 1:] for sub in connectionString.split(";")]
            self.iotHost = iotHost
            self.keyName = keyName
            self.keyValue = keyValue
            
    def _buildExpiryOn(self):
        return '%d' % (time.time() + self.TOKEN_VALID_SECS)
    
    def _buildIoTHubSasToken(self, deviceId):
        resourceUri = '%s/devices/%s' % (self.iotHost, deviceId)
        targetUri = resourceUri.lower()
        expiryTime = self._buildExpiryOn()
        toSign = '%s\n%s' % (targetUri, expiryTime)
        key = base64.b64decode(self.keyValue.encode('utf-8'))
        signature = urllib.quote(
            base64.b64encode(
                hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
            )
        ).replace('/', '%2F')
        return self.TOKEN_FORMAT % (signature, expiryTime, self.keyName, targetUri)
    
    def sendD2CMsg(self, deviceId, message):
        sasToken = self._buildIoTHubSasToken(deviceId)
        url = 'https://%s/devices/%s/messages/events?api-version=%s' % (self.iotHost, deviceId, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message)
        return r.text, r.status_code
    
if __name__ == '__main__':
    connectionString = 'HostName={{cookiecutter.hub_name}}.azure-devices.net;SharedAccessKeyName=device;SharedAccessKey={{cookiecutter.device_policy_key}}'
    d2cMsgSender = D2CMsgSender(connectionString)
    deviceId = 'iotdevice1'
    message = 'Hello, IoT Hub'
    print d2cMsgSender.sendD2CMsg(deviceId, message)

    # store the device message to document db
    pydocutils.upsertDocument(D2CMsgSender.HOST, D2CMsgSender.KEY, D2CMsgSender.DATABASE, D2CMsgSender.COLLECTION, { 'id': deviceId, 'message': message })
