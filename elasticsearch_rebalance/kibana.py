import requests
from urllib.parse import urljoin, quote_plus
import json

class ClusterClient():
    def __init__(self, kibana: 'Kibana') -> None:
        self.kibana = kibana
    
    def health(self):
        return self.kibana.query("_cluster/health", method="GET")
    
    def reroute(self, commands: dict):
        return self.kibana.query("_cluster/reroute", method="POST", data=commands)
    
    def put_settings(self, settings: dict):
        return self.kibana.query("_cluster/settings", method="PUT", data=settings)

class NodeClient():
    def __init__(self, kibana: 'Kibana') -> None:
        self.kibana = kibana
    
    def stats(self):
        return self.kibana.query("_nodes/stats", method="GET")

class IndicesClient():
    def __init__(self, kibana: 'Kibana') -> None:
        self.kibana = kibana
    
    def get_settings(self):
        return self.kibana.query("_settings", method="GET")

class CatClient():
    def __init__(self, kibana: 'Kibana') -> None:
        self.kibana = kibana
    
    def shards(self, format: str = "json", bytes: str = "b"):
        return self.kibana.query(f"_cat/shards?format={format}&bytes={bytes}", method="GET")
    
    def nodeattrs(self, format: str = "json"):
        return self.kibana.query(f"_cat/nodeattrs?format={format}", method="GET")

    def recovery(self, active_only: bool = True, h: str = None, s: str = None, format: str = "json"):
        url = f"_cat/recovery?format={format}"
        if active_only:
            url += "&active_only=true"
        if h:
            url += f"&h={h}"
        if s:
            url += f"&s={s}"
        return self.kibana.query(url, method="GET")

class Kibana:
    def __init__(self, base_url, username, password, cookie) -> None:
        base_url_parsed = base_url if base_url[-1] != "/" else base_url[:-1]
        self.cookie = cookie
        self.username = username
        self.password = password
        self.base_url = base_url_parsed
        self.kibana_url = base_url_parsed if 'proxy' in base_url_parsed else urljoin(base_url_parsed, '/api/console/proxy')
        self.cluster = ClusterClient(self)
        self.nodes = NodeClient(self)
        self.indices = IndicesClient(self)
        self.cat = CatClient(self)

    def generate_url(self, path: str, method: str = "GET") -> str:
        # encode path
        encoded_path = quote_plus(path)
        method_parsed= method.upper()
        url = f"?path={encoded_path}&method={method_parsed}"
        return urljoin(self.kibana_url, url)

    def query(self, path: str, method: str = "get", data: dict = None) -> requests.Response:
        url = self.generate_url(path, method)
        return self.request(url=url, json=data)
    
    def request(self, url: str, method: str = "post", **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers.update({
            'Kbn-Xsrf': 'kibana'
        })
        if self.cookie:
            headers.update({
                'Cookie': self.cookie
            })
        elif self.username and self.password:
            kwargs.update({
                'auth': (self.username, self.password)
            })
        
        response = requests.request(method=method, url=url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()