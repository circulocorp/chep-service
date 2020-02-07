from suds.client import Client
import xmltodict


class Chep(object):

    def __init__(self, username, password, url):
        self._user = username
        self._pass = password
        self._url = url
        self._client = None

    def _gen_client(self):
        if self._client is None:
            print(self._url)
            self._client = Client(self._url)

    def send_events(self, events):
        self._gen_client()
        xml = xmltodict.unparse(events)
        resp = self._client.service.ProcessXML(xml)
        if resp == "Ok":
            return "Ok"
        else:
            return xml



