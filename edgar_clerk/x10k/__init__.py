import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from ..ref_data.enum import XBRLEnum
from ..request_headers import HEADERS


def parse_txt(txt):
    try:
        parsed_txt = float(txt)
    except (TypeError, ValueError):
        parsed_txt = txt
    return parsed_txt


class XBRL10KQParser(object):

    def __init__(self, cik, filing_type=XBRLEnum.X10K.value, limit=None, dateb=None):
        self.cik = cik
        self.limit = limit
        self.dateb = self._parse_date(dateb)
        filings_url_params = {
            'action': XBRLEnum.COMPANY.value,
            'CIK': self.cik,
            'type': filing_type
        }
        self.filings_response = requests.get(self.browse_url, params=filings_url_params, headers=HEADERS)
        self.xbrl_urls = self.get_xbrl_urls(self.filings_response.text)

    @property
    def browse_url(self):
        return 'https://www.sec.gov/cgi-bin/browse-edgar'

    @property
    def url(self):
        return 'https://www.sec.gov'

    @classmethod
    def _parse_date(cls, eff_date):
        if isinstance(eff_date, datetime.datetime):
            eff_date = eff_date.date()
        elif isinstance(eff_date, datetime.date):
            pass
        elif isinstance(eff_date, str):
            eff_date = datetime.datetime.strptime(eff_date, '%Y%m%d')
            eff_date = eff_date.date()
        elif eff_date is None:
            eff_date = datetime.date.today()
        return eff_date

    def get_xbrl_urls(self, edgar_str):
        soup = BeautifulSoup(edgar_str, 'html.parser')
        link_tags = soup.find_all('a', id='documentsbutton')
        doc_links = [f'{self.url}{link_tag["href"]}' for link_tag in link_tags]
        xbrl_links = []
        for link in doc_links:
            doc_resp = requests.get(link, headers=HEADERS)
            if doc_resp.status_code == 200:
                doc_str = doc_resp.text
                soup = BeautifulSoup(doc_str, 'html.parser')
                amend_label = soup.find('div', id='formName')
                if amend_label and '[amend]' in amend_label.text.lower():
                    # Skip amended filings for now as I'm unsure of how to parse them
                    continue
                table_tag = soup.find('table', class_='tableFile', summary='Data Files')
                if table_tag:
                    # If this table_tag doesn't exist, then it's likely the old style SEC filing so ignore these for now
                    rows = table_tag.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) > 3:
                            if 'XBRL INSTANCE DOCUMENT' in cells[1].text:
                                xbrl_links.append('https://www.sec.gov' + cells[2].a['href'])
        return xbrl_links

    def get_unique_tags(self, num_links_to_check=1):
        num_links_to_check = num_links_to_check if num_links_to_check > 0 else 1
        if self.xbrl_urls:
            url_list = self.xbrl_urls[:num_links_to_check]
            unique_tags = set()
            for url in url_list:
                resp = requests.get(url, headers=HEADERS)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tag_list = [tag.name for tag in soup.find_all()]
                    unique_tags = unique_tags.union(tag_list)
            return unique_tags

    def get_filing_data(self, tag_list):
        date_tag_str = 'dei:documentperiodenddate'
        filing_results = []
        for url in self.xbrl_urls:
            resp = requests.get(url, headers=HEADERS)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                date_tag = soup.find(date_tag_str)
                doc_date = date_tag.text if date_tag else None
                if doc_date:
                    filing_result = {'date': doc_date}
                    for tag_str in tag_list:
                        tag = soup.find(tag_str)
                        extracted_txt = tag.text if tag else None
                        filing_result[tag_str] = parse_txt(extracted_txt)

                    filing_results.append(filing_result)
        filing_results_df = pd.DataFrame(filing_results)
        filing_results_df = filing_results_df.convert_dtypes()
        return filing_results_df
