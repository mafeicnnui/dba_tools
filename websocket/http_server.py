import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.options
import os,json
import datetime

from tornado.web import RequestHandler
from tornado.options import define, options
from tornado.websocket import WebSocketHandler

'''
WebSocket
WebSocket是HTML5规范中新提出的客户端-服务器通讯协议，协议本身使用新的ws://URL格式。

WebSocket 是独立的、创建在 TCP 上的协议，和 HTTP 的唯一关联是使用 HTTP 协议的101状态码进行协议切换，使用的 TCP 端口是80，可以用于绕过大多数防火墙的限制。

WebSocket 使得客户端和服务器之间的数据交换变得更加简单，允许服务端直接向客户端推送数据而不需要客户端进行请求，两者之间可以创建持久性的连接，并允许数据进行双向传送。

目前常见的浏览器如 Chrome、IE、Firefox、Safari、Opera 等都支持 WebSocket，同时需要服务端程序支持 WebSocket。

1. Tornado的WebSocket模块
Tornado提供支持WebSocket的模块是tornado.websocket，其中提供了一个WebSocketHandler类用来处理通讯。

WebSocketHandler.open()
当一个WebSocket连接建立后被调用。

WebSocketHandler.on_message(message)
当客户端发送消息message过来时被调用，注意此方法必须被重写。

WebSocketHandler.on_close()
当WebSocket连接关闭后被调用。

WebSocketHandler.write_message(message, binary=False)
向客户端发送消息messagea，message可以是字符串或字典（字典会被转为json字符串）。若binary为False，则message以utf8编码发送；二进制模式（binary=True）时，可发送任何字节码。

WebSocketHandler.close()
关闭WebSocket连接。

WebSocketHandler.check_origin(origin)
判断源origin，对于符合条件（返回判断结果为True）的请求源origin允许其连接，否则返回403。可以重写此方法来解决WebSocket的跨域请求（如始终return True）。
'''

class IndexHandler(RequestHandler):
    def get_current_user(self):
        '''
        重写RequestHandler类中的get_current_user方法，用来判断当前是否是登录状态，请求中所有被@tornado.web.authenticated 装饰的方法，都需要此方法返回值不为None，否则给与403拒绝
        :return: 用户名或者None . 为None判断为非法请求，POST 时Tornado进行403禁止访问 ；GET 时 302 重定向到/login
        '''
        user = self.get_argument(name='username',default='None')
        if user and user != 'None':
            print('IndexHandler类 get_current_user获取到用户:',user)
            return user

    @tornado.web.authenticated   #确认请求合法 依赖于get_current_user(self):函数的返回值作为判断请求是否合法
    def get(self):
        print("IndexHandler 收到GET请求")
        self.render("online_index.html",current_user=self.current_user)

    @tornado.web.authenticated
    def post(self, *args, **kwargs):
        print('IndexHandler 收到POST请求')
        self.render("online_index.html", current_user=self.current_user)

class LoginHandler(RequestHandler):
    def get(self, *args, **kwargs):
        '''
        处理输入昵称界面get请求
        :param args:
        :param kwargs:
        :return:
        '''
        cookie_value = self.get_secure_cookie('count')
        print('cookie_value :', cookie_value)
        count = int(cookie_value) + 1 if cookie_value else 1
        self.set_secure_cookie("count", str(count))  # 设置一个带签名和时间戳的cookie，防止cookie被伪造。

        #使用ajax方法做的前端
        # self.render('login_use_ajax.html')
        #使用form表单提交数据 的前端
        self.render('login_use_form.html')
    def post(self, *args, **kwargs):
        '''
        暂时用不到
        :param args:
        :param kwargs:
        :return:
        '''
        pass

 # 继承tornado.websocket.WebSocketHandler,只处理WS协议的请求
class ChatHandler(WebSocketHandler):
    def get_current_user(self):
        user = self.get_argument(name='username',default='None')
        if user and user != 'None':
            return user

    users = set()  # 用来存放在线用户的容器
    @tornado.web.authenticated
    def open(self):
        print('收到新的WebSocket连接')
        self.users.add(self)  # 建立连接后添加用户到容器中
        for u in self.users:  # 向已在线用户发送消息
            u    .write_message(u"[%s]-[%s]-%s 进入聊天室" % (
    self.request.remote_ip, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.current_user))

    def on_message(self, message):
        message = json.loads(message)
        print(type(message),message)
        for u in self.users:  # 向在线用户广播消息
            u.write_message(u"[%s]-[%s]-说：<br> &nbsp&nbsp&nbsp&nbsp%s" % ( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.current_user,message.get('msg')))

    def on_close(self):
        self.users.remove(self) # 用户关闭连接后从容器中移除用户
        for u in self.users:
            u.write_message(u"[%s]-[%s]-%s 离开聊天室" % (self.request.remote_ip, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.current_user))

    def check_origin(self, origin):
        return True  # 允许WebSocket的跨域请求

if __name__ == '__main__':
    tornado.options.parse_command_line()    #允许命令行启动程序

    app = tornado.web.Application([         #定义处理web请求的应用程序
            (r"/", IndexHandler),
            (r"/login", LoginHandler),
            (r"/chat", ChatHandler),        # 处理WebSocket协议传输的数据
        ],
        websocket_ping_interval = 5, # WebSocket ping探活包发送间隔秒数
        static_path = os.path.join(os.path.dirname(__file__), "statics"),           #配置应用程序前端所需静态文件目录
        template_path = os.path.join(os.path.dirname(__file__), "templates"),       #配置html文件路径
        login_url='/login',                                                         #配置登录url
        xsrf_cookies=True,                                                          #,防止跨站请求攻击，在post请求中起效,
        cookie_secret="2hcicVu+TqShDpfsjMWQLZ0Mkq5NPEWSk9fi0zsSt3A=",               # 安全cookie用预置秘钥 base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)
        debug = True                                                                #配置调试级别
        )
    http_server = tornado.httpserver.HTTPServer(app)                                #将应用处理逻辑 传递给HTTPServer 服务
    define("port", default=8000, type=int)                                          #设置一个监听地址
    http_server.listen(options.port)                                                #配置监听地址到 HTTPServe
    tornado.ioloop.IOLoop.current().start()