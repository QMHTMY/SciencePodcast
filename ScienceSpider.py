#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re
import os
import sys
import time
import logging
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
        self.headers    = {'User-Agent':'Mozilla/5.0 (compatible; MSTE 5.5; Windows NT)', 'Connection':'close'}
        self.podprefix  = 'Science-'                     #音频前缀名
        self.showstr    = 'downloading from Science... ' #下载进度显示字符
        self.pg_bs_url  = "https://www.sciencemag.org/podcasts?page=" #page_base_url
        self.pt_bs_url  = "https://www.sciencemag.org"   #podcast_base_url
        self.max_job    = max_job                        #分布式下载进程最大数量
        self.storedir   = storedir                       #设定存储位置名称
        self.max_page   = self._get_max_page()
        self.page_urls  = self._get_page_urls() 

        if not path.exists(self.storedir):
            os.makedirs(self.storedir)


    #*********************1.准备文件保存目录和每一页的链接********************
    def _get_max_page(self):
        max_page = None
        url = self.pg_bs_url + '0'
        soup = self._get_url_content(url)
        if soup:
            last_patn = 'pager-last ellipsis last'  #硬编码，不好
            rest = soup.find('li',class_=last_patn) 
            max_page = int(rest.getText()[:2])
        return max_page 

    def _get_page_urls(self):
        if self.max_page == None:
            return []
        else:
            return [self.pg_bs_url + str(pg_nm) for pg_nm in range(self.max_page)]


    #*********************2.提取播客/pdf的链接及其保存名字********************
    def _get_podcast_info(self,url):
        radio_url  = None
        pdf_url    = None 
        radio_name = None
        pdf_name   = None 

        soup = self._get_url_content(url)
        if soup:
            radio_url, pdf_url   = self._get_podcast_url(soup)
            radio_name, pdf_name = self._get_podcast_name(soup) 

            info_dic = {'radio':[radio_url,radio_name],'pdf':[pdf_url,pdf_name]}
        else:
            info_dic = {'radio':[radio_url,radio_name],'pdf':[pdf_url,pdf_name]}

        return info_dic 

    def _get_podcast_url(self,soup):
        patn = re.compile(r'http://(.*)\.mp3')
        link = soup.find('a',href=patn)
        if link == None:
            radio_url  = None
        else:
            radio_url  = link['href'] 

        patn = re.compile(r'https://(.*)\.pdf')
        link = soup.find('a',href=patn)
        if link == None:
            pdf_url  = None
        else:
            pdf_url  = link['href'] 

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
        if (url == None) or path.exists(fl_name):
            return None

        with closing(get(url,stream=True,headers=self.headers,timeout=5)) as res:
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
        for url in set(urls):      #url去重
            pool.apply_async(self._func,(url,))

        pool.close()
        pool.join()

    def _func(self,url):
        info_dic = self._get_podcast_info(url)
        radio_info = info_dic['radio']
        pdf_info   = info_dic['pdf']
        self._download_file(radio_info[0],radio_info[1])
        self._download_file(pdf_info[0],pdf_info[1])


    #*********************5.提取页面播客链接**********************************
    def _get_podcast_urls(self,url):
        #linksf = soup.find_all('h2',class_="media__headline")

        soup = self._get_url_content(url)
        if soup:
            patn = re.compile(r'/podcast/(.*)?')
            links= soup.find_all('a',href=patn)

            pdcst_urls = [''.join([self.pt_bs_url + lk['href']]) for lk in links]
            return pdcst_urls 
        else:
            return []


    #*********************6.下载启动器****************************************
    def _get_url_content(url):
        '''网页下载函数'''
        html_res = get(url,headers=self.headers)
        if 200 == html_res.status_code:
            html_res.encoding='utf-8'
            soup = Soup(html_res.text,'html.parser') 
            return soup
        else:
            return None

    def control(self):
        for page_url in self.page_urls:
            podcast_urls = self._get_podcast_urls(page_url)
            self._download_multi(podcast_urls)
            time.sleep(5)

if __name__ == "__main__":
    logging.disable(logging.CRITICAL)                  #调试开关
    start = time.time()

    spider = Spider()
    try:
        spider.control()
    except Exception as err:
        print(err)
    finally:
        end = time.time() 
        last = (end - start)/60
        print("Time consumed:%.2f minute(s)"%(last))
