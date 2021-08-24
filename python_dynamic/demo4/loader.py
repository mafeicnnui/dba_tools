import traceback,types

class loader(object):
	def __init__(self):
		pass

	def load(self,name,path):
		try:
			m = types.ModuleType(name)
			exec(open(path).read(),m.__dict__)
			return m
		except:
			print("Load module [path {}] error: {}".format(path,traceback.format_exc()))
			return None


load = loader()
m = load.load("test","test_func.py")
print(m)
print(m.__dict__)
m.test()		