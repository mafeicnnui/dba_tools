#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/9/9 14:04
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

'''
appId：27401847
apikey:dqkZ0cZ5ON5ZiTua3cFEGznk
Secret Key:lCHHXMd9bvlbI5kLiXdAeQwgKnUezkDr
'''

from aip import AipImageClassify

# 合商云汇
# APP_ID = '27401847'
# API_KEY = 'dqkZ0cZ5ON5ZiTua3cFEGznk'
# SECRET_KEY = 'lCHHXMd9bvlbI5kLiXdAeQwgKnUezkDr'

# 马飞个人
# APP_ID = '27402567'
# API_KEY = 'HXLtMuVzA3zORIVFLN4rl2Lh'
# SECRET_KEY = 'evr4xtVbkwFSOdIh2wvvgDMkw1sHP3mf'
# client = AipImageClassify(APP_ID, API_KEY, SECRET_KEY)
#
# url = "http://hopson-park-pro.oss-cn-beijing.aliyuncs.com/20220909140004382_%E4%BA%ACQ95XQ5_plate.jpg"
# res = client.carDetectUrl(url, {})
# print(res)


'''
APP_ID = '27401847'
API_KEY = 'dqkZ0cZ5ON5ZiTua3cFEGznk'   # 应用
SECRET_KEY = 'lCHHXMd9bvlbI5kLiXdAeQwgKnUezkDr' # 应用
{'error_code': 18, 'error_msg': 'Open api qps request limit reached'}

APP_ID = '27401847'
API_KEY = '1a031d535e3b4e4ba57a8174e46'    # 用户-安全认证
SECRET_KEY = '688c3b95319e41fc80baca529d989bf0' # 用户-安全认证

{'error_code': 14, 'error_msg': 'IAM Certification failed'}

import requests

host = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=dqkZ0cZ5ON5ZiTua3cFEGznk&client_secret=lCHHXMd9bvlbI5kLiXdAeQwgKnUezkDr'
response = requests.get(host)
if response:
   print(response.json())
   
{
    'refresh_token': '25.b16b917a9799aa6986a031f6c49a977a.315360000.1978071165.282335-27401847', 
    'expires_in': 2592000, 
    'session_key': '9mzdWujohR0FZ9re5zyPvJvqOTpZ22GHOY0Y6rhZIiupUczMC+Y6tGV0G6scTATT4buZHGj6U6bs43kH8I1YS5PBRnpl3A==', 
    'access_token': '24.a45bf6c6584cdb69aa87eaece5632d20.2592000.1665303165.282335-27401847', 
    'scope': 'public vis-classify_dishes vis-classify_car brain_all_scope vis-classify_animal vis-classify_plant brain_object_detect brain_realtime_logo brain_dish_detect brain_car_detect brain_animal_classify brain_plant_classify brain_ingredient brain_advanced_general_classify brain_custom_dish brain_poi_recognize brain_vehicle_detect brain_redwine brain_currency brain_vehicle_damage brain_multi_ object_detect brain_vehicle_attr wise_adapt lebo_resource_base lightservice_public hetu_basic lightcms_map_poi kaidian_kaidian ApsMisTest_Test权限 vis-classify_flower lpq_开放 cop_helloScope ApsMis_fangdi_permission smartapp_snsapi_base smartapp_mapp_dev_manage iop_autocar oauth_tp_app smartapp_smart_game_openapi oauth_sessionkey smartapp_swanid_verify smartapp_opensource_openapi smartapp_opensource_recapi fake_face_detect_开放Scope vis-ocr_虚拟人物助理 idl-video_虚拟人物助理 smartapp_component smartapp_search_plugin avatar_video_test b2b_tp_openapi b2b_tp_openapi_online smartapp_gov_aladin_to_xcx', 
    'session_secret': 'eb1b996e430e7b6b24d69307452b7a37'
}


'''


#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 13:34
# @Author : ma.fei
# @File : http_server.py.py
# @func :  Simple http server
# @Software: PyCharm


import os
import json
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
import warnings

# 控制器
class car(tornado.web.RequestHandler):
     def get(self):
         image = self.get_argument("url")
         APP_ID = '27402567'
         API_KEY = 'HXLtMuVzA3zORIVFLN4rl2Lh'
         SECRET_KEY = 'evr4xtVbkwFSOdIh2wvvgDMkw1sHP3mf'
         client = AipImageClassify(APP_ID, API_KEY, SECRET_KEY)
         #url = "http://hopson-park-pro.oss-cn-beijing.aliyuncs.com/20220909140004382_%E4%BA%ACQ95XQ5_plate.jpg"
         url = image
         print('url=',url)
         res = client.carDetectUrl(url, {})
         self.write(json.dumps(res))

# http server
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/car", car),
        ]
        settings = dict(
            static_path=os.path.join(os.path.dirname(__file__), "static"),  # 配置静态资源 js,css
            template_path=os.path.join(os.path.dirname(__file__), "templates"),  # 前端页面存放位置
            debug=True )
        tornado.web.Application.__init__(self, handlers,**settings)


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(9800)
    print('Web Server running...')
    print('Access add user: http://10.2.39.21:9800/car')
    tornado.ioloop.IOLoop.instance().start()



