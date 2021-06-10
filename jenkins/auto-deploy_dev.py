#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/30 16:37
# @Author : 马飞
# @File : auto-deploy_dev.py
# @Software: PyCharm

import jenkins  #引入jenkins类库
import time
import traceback
import sys

class Jenkins_task:
    def __init__(self,jenkins_url,user,password):
        self.jenkins_url = jenkins_url    #jenkins网页url
        # self.job_name = job_name          #任务名称
        self.user     = 'admin'           #登陆jenkins网页端用户名
        self.password = '115eaaae84bf5e6ff967e53d19e0797816' #'hopson@2019'      #登陆jenkins网页端秘密
        #self.password = 'hopson@2019'     # 'hopson@2019'      #登陆jenkins网页端秘密
        self.jenkins_login()              #执行jenkins_login函数
        self.server.get_whoami()

    def jenkins_login(self):    #登陆jenkins
        self.server = jenkins.Jenkins(url=self.jenkins_url,username=self.user,password=self.password)
        return self.server

    def get_version(self):    #获取用户名与版本
        user = self.user
        version = self.server.get_version()
        print('您好%s!您当前使用版本是：%s' % (user,version))
        # config = self.server.get_job_config(self.job_name)
        # print('您好%s!您当前使用配置是：%s' % (user, config))

    def get_git_address(self):    #获取job任务git地址
        user = self.user
        config = self.server.get_job_config(self.job_name)
        #print('您好%s!您当前使用配置是：%s' % (user, config))
        url =config.split('<hudson.plugins.git.UserRemoteConfig>')[1].split('<url>')[1].split('</url>')[0]
        return url

    def get_conf_shell(self):    #获取job任务shell脚本
        user = self.user
        config = self.server.get_job_config(self.job_name)
        #print('您好%s!您当前使用配置是：%s' % (user, config))
        shell =config.split('<hudson.tasks.Shell>')[2].split('<command>')[1].split('</command>')[0]
        return shell


    def create_job(self,job_conf):    #创建任务
        try:
            self.server.create_job(self.job_name,job_conf)
            print('任务已创建:%s' % self.job_name)
        except Exception as error:
            return error

    def set_job_config(self,config_xml):   #修改job配置
        self.server.reconfig_job(self.job_name, config_xml=config_xml)
        print('任务配置已变更:{}'.format(self.job_name))


    def set_job_name(self,job_name):   #修改job配置
        self.job_name = job_name


    def get_job_list(self):    #获取任务列表
        job_list = self.server.get_jobs()
        my= []
        for i in job_list:
            my.append(i['name'])
        return my

    def del_job(self):    #删除任务
        try:
            self.server.delete_job(self.job_name)
            print("该项目已删除:%s" % self.job_name)
        except jenkins.NotFoundException as error:
            return error

    def build_job(self):    #构建任务
        try:
            param_dict = {'Branch': 'origin/develop'}
            self.server.build_job(self.job_name,param_dict)
            #self.server.build_job(self.job_name)
            print('正在构建任务%s...' % self.job_name)
        except jenkins.JenkinsException as error:
            traceback.print_exc()
            return error

    def get_build_job_status(self):    #获取任务构建状态
        next_build_number = self.server.get_job_info(self.job_name)['nextBuildNumber']    #获取构建任务号码，这个号码是唯一的
        time.sleep(10)    #睡10秒，因为有时候jenkins还没反应过来代码已经走到这了，因此会找不到该任务
        get_build_console_output = self.server.get_build_console_output(self.job_name,next_build_number)    #获取控制台输出
        status = self.server.get_build_info(self.job_name,next_build_number)
        while True:
            if status != None and str(status['result']) == "SUCCESS":
                print('%s构建任务已完成'% self.job_name)
                return get_build_console_output
            else:
                if str(status['building']) == True:
                    continue
                else:
                    print('Error：构建过程中发现未知错误!')
                    return get_build_console_output

default_xml = '''<?xml version='1.1' encoding='UTF-8'?>
<project>
  <actions/>
  <description></description>
  <keepDependencies>false</keepDependencies>
  <properties>
    <jenkins.model.BuildDiscarderProperty>
      <strategy class="hudson.tasks.LogRotator">
        <daysToKeep>-1</daysToKeep>
        <numToKeep>30</numToKeep>
        <artifactDaysToKeep>-1</artifactDaysToKeep>
        <artifactNumToKeep>-1</artifactNumToKeep>
      </strategy>
    </jenkins.model.BuildDiscarderProperty>
    <hudson.model.ParametersDefinitionProperty>
      <parameterDefinitions>
        <net.uaznia.lukanus.hudson.plugins.gitparameter.GitParameterDefinition plugin="git-parameter@0.9.11">
          <name>Branch</name>
          <description></description>
          <uuid>24d71104-cc4f-4ec3-a1ea-ed835a5b8731</uuid>
          <type>PT_BRANCH</type>
          <branch>origin/develop</branch>
          <tagFilter>origin/develop</tagFilter>
          <branchFilter>origin/develop</branchFilter>
          <sortMode>NONE</sortMode>
          <defaultValue></defaultValue>
          <selectedValue>NONE</selectedValue>
          <quickFilterEnabled>false</quickFilterEnabled>
          <listSize>5</listSize>
        </net.uaznia.lukanus.hudson.plugins.gitparameter.GitParameterDefinition>
        <hudson.model.ChoiceParameterDefinition>
          <name>ENV</name>
          <description>发布环境，可选开发、测试、演示。</description>
          <choices class="java.util.Arrays$ArrayList">
            <a class="string-array">
              <string>develop</string>
            </a>
          </choices>
        </hudson.model.ChoiceParameterDefinition>
      </parameterDefinitions>
    </hudson.model.ParametersDefinitionProperty>
  </properties>
  <scm class="hudson.plugins.git.GitSCM" plugin="git@3.12.1">
    <configVersion>2</configVersion>
    <userRemoteConfigs>
      <hudson.plugins.git.UserRemoteConfig>
        <url>http://git.hopson.io:9999/hopsonone/backend-server/one-process-job.git</url>
        <credentialsId>87484229-8a1c-43c2-8dec-f1ef2a02948e</credentialsId>
      </hudson.plugins.git.UserRemoteConfig>
    </userRemoteConfigs>
    <branches>
      <hudson.plugins.git.BranchSpec>
        <name>origin/develop</name>
      </hudson.plugins.git.BranchSpec>
    </branches>
    <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
    <submoduleCfg class="list"/>
    <extensions/>
  </scm>
  <canRoam>true</canRoam>
  <disabled>false</disabled>
  <blockBuildWhenDownstreamBuilding>false</blockBuildWhenDownstreamBuilding>
  <blockBuildWhenUpstreamBuilding>false</blockBuildWhenUpstreamBuilding>
  <triggers/>
  <concurrentBuild>false</concurrentBuild>
  <builders>
    <hudson.tasks.Shell>
      <command>/home/hopson/apps/var/jenkins/scripts/notice.sh</command>
    </hudson.tasks.Shell>
    <hudson.tasks.Maven>
      <targets>clean
compile
package
-Dmaven.test.skip=true</targets>
      <mavenName>maven</mavenName>
      <usePrivateRepository>false</usePrivateRepository>
      <settings class="jenkins.mvn.DefaultSettingsProvider"/>
      <globalSettings class="jenkins.mvn.DefaultGlobalSettingsProvider"/>
      <injectBuildVariables>false</injectBuildVariables>
    </hudson.tasks.Maven>
    <hudson.tasks.Shell>
      <command>/home/hopson/apps/var/jenkins/scripts/linshi.sh</command>
    </hudson.tasks.Shell>
  </builders>
  <publishers/>
  <buildWrappers>
    <org.jenkinsci.plugins.builduser.BuildUser plugin="build-user-vars-plugin@1.5"/>
  </buildWrappers>
</project>'''

# job_config = [
#     {
#         'job_name'  :'one-process-job',
#         'job_branch':'origin/develop',
#         'job_git'   :''
#
#     },
#     {
#         'job_name'  : 'one-process-cms',
#         'job_branch': 'origin/develop',
#         'job_git': ''
#
#     },
#     {
#         'job_name': 'mall-goods-stock',
#         'job_branch': 'origin/develop',
#         'job_git': ''
#     },
#
# ]

#job_list = ['one-process-job','one-process-cms','mall-goods-stock']
#job_list = ['one-process-job']

job_list = ['hopsonone-coupons-api','hopsonone-coupons-cms','hopsonone-coupons-dcs','hopsonone-coupons-push','hopsonone-coupons-search','hopsonone-coupons-service','mall-order-service','one-flash-sale-api','one-flash-sale-data-service','one-flash-sale-dcs','one-member-marketing-cms','one-card-api','one-card-cms','one-business-api','one-business-cms','one-business-dcs','one-park-api','one-park-cms','park-carinout','park-booking','one-cms-dcs','one-base-api','one-personal-api','one-activity-api','one-homepage-api','one-notify-service','one-points-api','one-cms','one-push-api','one-order-center-service','one-market-analysis','weixin-miniapp','one-activity-cms','one-activity-job','one-games-cms','mall-goods-service','mall-goods-dcs','mvms-service','mvms-cms-service','mvms-dcs']


jenkins_url  = 'http://10.2.39.55:8080'
jenkins_user = 'admin'  # 登陆jenkins网页端用户名
jenkins_password = '115eaaae84bf5e6ff967e53d19e0797816'  # 'hopson@2019'      #登陆jenkins网页端秘密

server = Jenkins_task(jenkins_url,jenkins_user,jenkins_password)
server.get_version()

all_jobs=server.get_job_list()
print('')
print('print all job...')
print('-'.ljust(125,'-'))
for job in all_jobs:
    print(job)

# for cfg in job_config:
#     server.set_job_name(cfg['job_name'])
#     cfg['job_git'] = server.get_git_address()
#
# for cfg in job_config:
#      print(cfg)

print('')
print('print match job...')
print('-'.ljust(125,'-'))

flag = True
no_exists = []
for job in job_list:
    if job in all_jobs:
        server.set_job_name(job)
        print(job, server.get_conf_shell())
    else:
        no_exists.append(job)
        flag = False

print('')
print('print not exists job...')
print('-'.ljust(125,'-'))
for i in no_exists:
    print('job :`{}`'.format(i))


if flag == False:
    print('sys exit !'.format(job))
    sys.exit(0)

# print('')
# print('Beginning deploying ...\n')
# for job in job_list:
#     if job in all_jobs:
#         server.set_job_name(job)
#         print(job,server.get_conf_shell())

# print('')
# print('Beginning deploying ...\n')
# for job in job_list:
#     if job in all_jobs:
#         print('deploying job {}...'.format(job))
#         server.set_job_name(job)
#         server.build_job()
#         print(server.get_build_job_status())

# server.del_job()
# server.create_job(default_xml)
#server.set_job(config_xml=default_xml)
# server.build_job()
# print(server.get_build_job_status())

