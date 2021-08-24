from test_class import test

class test1(test):
	def __init__(self):
		test.__init__(self)

	def add(self,x,y):
	    return x+y