'''
  功能:爬取各项目上图片至minIO服务器
'''
import requests
import os
from bs4 import BeautifulSoup


def request_download(p_root,p_dir,p_url):
    r = requests.get(p_url)
    f = p_url.split('/')[-1]
    p = '/'.join(p_url.split('/')[0:-1]).replace(p_root,'')
    if not os.path.isdir(os.path.join(p_dir,p)):
       os.makedirs(os.path.join(p_dir,p))
       print('create directory {} ok'.format(os.path.join(p_dir,p)))
    with open(os.path.join(p_dir,p,f), 'wb') as f:
        f.write(r.content)

def get_url(p_root,p_url,p_img):
    html_contents = requests.get(p_url).text
    soup  = BeautifulSoup(html_contents, 'html5lib')
    a_tag = soup.find_all('a')

    if  a_tag == []:
        return

    for k in a_tag:
        n_url = p_root+k['href'][1:]
        if len(n_url)>len(p_url) :
           if n_url.find('.jpg')>0 or n_url.find('.png')>0:
              p_img.append(n_url)
           else:
              get_url(p_root, n_url,p_img)


'''
  1.将下载图片、上片minIO花费时间写入数据库中
  2.测试1天，7天两种情况
  3.从项目至公司内网，从项目至阿里云
'''

if __name__ == '__main__':
    root = "http://10.5.3.23:811/"
    url  = "http://10.5.3.23:811/Upload/Images/2020/1/1/"
    dir  = "/home/hopson/apps/usr/webserver/dba/downloads"
    img  = []
    print('Requesting remote image ...')
    get_url(root,url,img)

    for p in img:
        print('Downloading image:{}'.format(p))
        request_download(root,dir,p)

