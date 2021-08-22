import requests
from functools import lru_cache
from .enum import XBRLEnum
from ..request_headers import HEADERS


@lru_cache()
def get_company_name_cik_dict():
    all_companies_page = requests.get("https://www.sec.gov/Archives/edgar/cik-lookup-data.txt",
                                      headers=HEADERS)
    all_companies_content = all_companies_page.content.decode("latin1")
    all_companies_array = all_companies_content.split("\n")
    del all_companies_array[-1]
    all_companies_dict = {}
    for name_cik in all_companies_array:
        name_cik_split = name_cik.split(':')
        if len(name_cik_split) == 3:
            name, cik, _ = name_cik_split
            all_companies_dict[name] = cik
    return all_companies_dict


class CIKDictInstance(object):

    def __init__(self):
        self._cik_dict = None

    def reload_cik_dict(self):
        self._cik_dict = get_company_name_cik_dict()

    @property
    def dict(self):
        return self._cik_dict

    def get_cik_dict_subset(self, search_str):
        return {key: val for key, val in self.dict.items() if search_str.lower() in key.lower()}


class CIKDict(object):
    _cik_dict = None

    def __new__(cls):
        if cls._cik_dict is None:
            cls._cik_dict = CIKDictInstance()
            cls._cik_dict.reload_cik_dict()
        return cls._cik_dict
