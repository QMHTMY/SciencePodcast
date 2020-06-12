#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#    Author: Shieber
#    Date: 2019.04.30
#
#                             APACHE LICENSE
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
#                            Function Description
#    download podcasts from Science.com website.   
#
#    Copyright 2019 
#    All Rights Reserved!

import re
import os
import sys
import time
import logging
import requests 
import os.path as path
from bs4 import BeautifulSoup as Soup
from tqdm import tqdm
from requests import get
from contextlib import closing
from urllib.parse import urljoin
from multiprocessing import Pool
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s-%(message)s')

class Spider():
    def __init__(self,max_job=5,storedir='Science/'):
        self.headers    = {
                            'User-Agent':'Mozilla/5.0 (compatible; MSTE 5.5; Windows NT)',
                            'Connection':'close'}
        self.downloaded = 0                                           #下载数
        self.podprefix  = 'Science-'                                  #音频前缀
        self.showstr    = 'downloading from Science... '              #下载进度显示字符
        self.pt_bs_url  = "https://www.sciencemag.org"                #podcast_base_url
        self.pg_bs_url  = "https://www.sciencemag.org/podcasts?page=0"#page_base_url
        self.max_job    = max_job                                     #下载进程最大数量
        self.storedir   = storedir                                    #设定存储位置名称
        self.page_urls  = self.get_page_urls()

        if not path.exists(self.storedir):
            os.makedirs(self.storedir)

    def get_page_urls(self):
        '''
            提取器A: 提取最大页数，构造所有
            页面的url列表page_urls并返回
        '''
        page_urls = []

        soup = self._get_url_content(self.pg_bs_url)
        if not soup:
            return page_urls 

        page_patn = 'pager-last ellipsis last'        #硬编码，不好
        match_res = soup.find('li', class_=page_patn) 
        max_page  = int(match_res.getText()[:2])      #最大页符号中类似28>>，
        for page_num in range(max_page):
            page_url = self.pg_bs_url.replace('0',str(page_num))
            page_urls.append(page_url)

        return page_urls


    #*********************2.提取播客/pdf的链接及其保存名字********************
    def _get_podcast_info(self,url):
        info_dic   = {'radio':[],'pdf':[]}

        soup = self._get_url_content(url)
        if soup:
            radio_url, pdf_url   = self._get_podcast_url(soup)
            radio_name, pdf_name = self._get_podcast_name(soup) 

            info_dic['radio'] = [radio_url, radio_name]
            info_dic['pdf']   = [pdf_url, pdf_name]
        else:
            info_dic['radio'] = [None, None]
            info_dic['pdf']   = [None, None]

        return info_dic 

    def _get_podcast_url(self,soup):
        patn = re.compile(r'http://(.*)\.mp3')
        link = soup.find('a',href=patn)
        if link:
            radio_url  = link['href'] 
        else:
            radio_url  = None

        patn = re.compile(r'https://(.*)\.pdf')
        link = soup.find('a',href=patn)
        if link:
            pdf_url  = link['href'] 
        else:
            pdf_url  = None

        return radio_url, pdf_url

    def _get_podcast_name(self,soup):
        timestr = soup.find('time').getText()

        timestr = timestr.replace(',','').replace('.','') 
        timestr = timestr.split()[:3]
        middle  = ''.join([self.podprefix,'-'.join(timestr)])

        radio_name = ''.join([self.storedir, middle,'.mp3'])
        pdf_name   = ''.join([self.storedir, middle,'.pdf'])

        return radio_name, pdf_name


    #*********************3.播客和pdf文件下载器*******************************
    def _download_file(self,url,fl_name):
        if (not url) or path.exists(fl_name):
            return None

        s = requests.session()
        s.keep_alive = False
        with closing(s.get(url,stream=True,headers=self.headers,timeout=5)) as res:
            if 200 == res.status_code:
                size = 1024*10

                cntnt_size = int(res.headers['content-length'])
                if path.exists(fl_name) and path.getsize(fl_name) >= cntnt_size:
                    return True 

                info = ''.join([self.showstr,path.basename(fl_name)])
                with open(fl_name,'wb') as stream:
                    for ck in tqdm(res.iter_content(chunk_size=size),ascii=True,desc=info):
                        stream.write(ck)


    #*********************4.分布式下载器**************************************
    def _download_multi(self,urls):
        '''分布式爬虫'''
        if urls == []:
            return 

        pool = Pool(self.max_job)
        for url in urls: 
            pool.apply_async(self._func,(url,))

        pool.close()
        pool.join()

    def _func(self,url):
        info_dic = self._get_podcast_info(url)
        radio_info = info_dic['radio']
        pdf_info   = info_dic['pdf']
        self._download_file(radio_info[0],radio_info[1]) #core function
        self._download_file(pdf_info[0],pdf_info[1])


    #*********************5.提取页面播客链接**********************************
    def _get_podcast_urls(self,url):
        #linksf = soup.find_all('h2',class_="media__headline")
        soup = self._get_url_content(url)
        if soup:
            patn = re.compile(r'/podcast/(.*)?')
            links= soup.find_all('a',href=patn)
            pdcst_urls = [''.join([self.pt_bs_url + lk['href']]) for lk in links]
            return set(pdcst_urls)
        return []


    #*********************6.下载启动器****************************************
    def _get_url_content(self,url):
        '''网页下载函数'''
        s = requests.session()
        s.keep_alive = False
        html_res = s.get(url,headers=self.headers)
        if 200 == html_res.status_code:
            html_res.encoding='utf-8'
            soup = Soup(html_res.text,'html.parser') 
            return soup
        return None

    def control(self):
        for page_url in self.page_urls:
            podcast_urls = self._get_podcast_urls(page_url)
            self._download_multi(podcast_urls)
            time.sleep(5)

if __name__ == "__main__":
    logging.disable(logging.CRITICAL)                  #调试开关
    start = time.time()
    requests.adapters.DEFAULT_RETRIES = 5
 
    spider = Spider()
    try:
        spider.control()
    except Exception as err:
        print(err)
    finally:
        end = time.time() 
        print(f"Downloaded in {(end - start)/60:.2f} minute(s)")
