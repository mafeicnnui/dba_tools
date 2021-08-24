import traceback

class loader(object):
	def __init__(self):
		pass

	def load(self,path):
		try:
			tmp = {}
			exec(open(path).read(),tmp)
			return tmp
		except:
			print("Load module [path {}] error: {}".format(path,traceback.format_exc()))
			return None
# 加载配置文件
load = loader()
m = load.load("test.conf")
addr = m['addr']
port = m['port']
print(addr+':'+str(port))

# 加载函数
load = loader()
m = load.load("test_func.py")
func = m['greeting']
func("world")

# 加载类
load = loader()
m = load.load("test_class.py")
c = m['test']
print(c)
print(dir(c))
t = c()
t.greeting("world")


# 加载子类
load = loader()
m = load.load("sub_test.py")
c = m['test1']
print(c)
print(dir(c))
t = c()
t.greeting("world")
v= t.add(10,20)
print(v)
