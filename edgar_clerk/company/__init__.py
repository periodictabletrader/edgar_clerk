import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from ..ref_data.enum import XBRLEnum
from ..request_headers import HEADERS
from ..utils import wrap_list


def parse_txt(txt):
    try:
        parsed_txt = float(txt)
    except (TypeError, ValueError):
        parsed_txt = txt
    return parsed_txt


class FilingsClerk(object):

    def __init__(self, cik, filing_types=None, limit=None, dateb=None):
        self.cik = cik
        self.limit = limit
        self.dateb = self._parse_date(dateb)
        filing_types = wrap_list(filing_types)
        self._filing_types = filing_types or [XBRLEnum.X10K]
        self._xbrl_urls = {}
        self._filings_responses = {}

    @property
    def filing_types(self):
        return self._filing_types

    @filing_types.setter
    def filing_types(self, new_filing_types):
        new_filing_types = wrap_list(new_filing_types)
        filtered_filing_types = \
            [filing_type for filing_type in new_filing_types if filing_type in XBRLEnum.COMPANY_FORM_TYPES]
        if filtered_filing_types:
            self._filing_types = filtered_filing_types
        else:
            raise Exception(
                f'Inappropriate Filing Types passed. Supported Filing Types are {XBRLEnum.COMPANY_FORM_TYPES}')

    @property
    def filings_responses(self):
        return self._filings_responses

    @property
    def xbrl_urls(self):
        if not self._xbrl_urls:
            request_params = {
                'action': XBRLEnum.COMPANY,
                'CIK': self.cik,
            }
            for filing_type in self.filing_types:
                request_params['type'] = filing_type
                response = requests.get(self.browse_url, params=request_params, headers=HEADERS)
                self._filings_responses[filing_type] = response
                self._xbrl_urls[filing_type] = self.get_xbrl_urls(response.text)
        return self._xbrl_urls

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
        xbrl_links = {}
        for link in doc_links:
            doc_resp = requests.get(link, headers=HEADERS)
            if doc_resp.status_code == 200:
                doc_str = doc_resp.text
                soup = BeautifulSoup(doc_str, 'html.parser')
                report_date_tags = soup.select('#formDiv > div.formContent > div:nth-child(2) > div.info')
                if report_date_tags:
                    report_date = report_date_tags[0].text
                    filing_date_tags = \
                        soup.select('#formDiv > div.formContent > div:nth-child(1) > div:nth-child(2)')
                    filing_date = filing_date_tags[0].text if filing_date_tags else None
                    existing_filing_date = xbrl_links[report_date][0] if report_date in xbrl_links else None
                    if existing_filing_date and filing_date < existing_filing_date:
                        # If a report link for a given report_date exists and has been filed at a later date than the
                        # current iteration's filing date, then it's likely an amendment which was filed later. Use the
                        # amended report link instead of the original
                        continue
                    table_tag = soup.find('table', class_='tableFile', summary='Data Files')
                    if table_tag:
                        # If this table_tag doesn't exist, then it's likely an old style SEC filing - ignore for now
                        rows = table_tag.find_all('tr')
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) > 3:
                                if 'XBRL INSTANCE DOCUMENT' in cells[1].text:
                                    xbrl_links[report_date] = ('https://www.sec.gov' + cells[2].a['href'], filing_date)
        return xbrl_links

    def get_unique_tags(self, filing_type=None, num_links_to_check=1):
        if filing_type is not None and filing_type not in self.filing_types:
            raise Exception(f'Invalid Filing Type used. This object can only pull {self.filing_types}')
        else:
            filing_type = self.filing_types[0]
        num_links_to_check = num_links_to_check if num_links_to_check > 0 else 1
        if self.xbrl_urls[filing_type]:
            url_list = []
            for num, url_tuple in enumerate(self.xbrl_urls[filing_type].items()):
                if num <= num_links_to_check:
                    url_list.append(url_tuple[1][0])
            unique_tags = set()
            for url in url_list:
                resp = requests.get(url, headers=HEADERS)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tag_list = [tag.name for tag in soup.find_all()]
                    unique_tags = unique_tags.union(tag_list)
            return unique_tags

    def get_filing_data(self, filing_type=None, tag_list=None):
        if filing_type is not None and filing_type not in self.filing_types:
            raise Exception(f'Invalid Filing Type used. This object can only pull {self.filing_types}')
        else:
            filing_type = self.filing_types[0]
        date_tag_str = 'dei:documentperiodenddate'
        tag_list = tag_list or []
        filing_results = []
        for report_date, url_info in self.xbrl_urls[filing_type].items():
            url, _ = url_info
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
