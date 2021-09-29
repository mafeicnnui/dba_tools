#/usr/bin/python
#coding=utf-8
import requests
import re
from lxml import etree
from multiprocessing import Pool
import gitlab

search_content='MALL-SHOP-SERVICE'
gitlab_url='http://47.94.203.190:9999'
private_token='FeYyzsjFNHtTRMHA7dzF'
gitlab_username='608545@hopson.com.cn'
gitlab_password='guxiao3618'

gl = gitlab.Gitlab(gitlab_url, private_token=private_token)
def create_session():
    session = requests.session()
    login_page_resp = session.get(gitlab_url + '/users/sign_in').text
    authenticity_token = re.findall(
        'type="hidden" name="authenticity_token" value="(.*?)" />',
        login_page_resp)[0]
    post_form = {
        'user[remember_me]': '0',
        'utf8': '?',
        'authenticity_token': authenticity_token,
        'user[login]': gitlab_username,
        'user[password]': gitlab_password}
    session.post(gitlab_url + '/users/sign_in', data=post_form)
    return session

urls =[]
content = create_session()

def getPorject(s):

    url="%s/search?utf8=&snippets=&scope=&search=%s&project_id=%s" % (gitlab_url,search_content,s)
    pagecontent=content.get(url).text
    pagehtml=etree.HTML(pagecontent)
    projecttitle=pagehtml.xpath("//div[@class='content']/div[@class='row-content-block']/a/@href")
    if len(projecttitle):
        print(gitlab_url+str(projecttitle[0]))
    else:
        return


if __name__ == '__main__':
    pool = Pool(processes=5)
    projects = gl.projects.list(all=True)
    print("projects size："+str(len(projects)))
    for i in projects:
        #getPorject(i.split()[0])
        pool.apply_async(func=getPorject, args=(i.id,))
    print('end')
    pool.close()
    pool.join()
