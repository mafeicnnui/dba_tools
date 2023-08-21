一、概述  

   基于python3.6+tornado简单web服务，实现用户增、删、查、改
   
二、安装部署  


2.1 安装python3环境 
    
    yum -y install python3
    

2.2 安装python3依赖  

    pip3 install -r requirements.txt -i https://pypi.douban.com/simple
    
2.3  数据库连接配置

    db.py 文件中

    db = MySQLDatabase('demo',host ='10.2.39.18',port=3306,user='puppet',passwd='Puppet@123')
    
        
2.5 数据库初始化
    
     运行db.py 文件可自动建表

三、停启服务


3.1 启动服务  

    sever.py 8400

