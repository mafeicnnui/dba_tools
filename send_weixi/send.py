#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/10/8 14:40
# @Author : ma.fei
# @File : send.py.py
# @Software: PyCharm

# 利用Python发送微信消息的方法
# pip3 install pyautogui
# pip3 install pyperclip

import os
import sys
import time
# 引入pyautogui模块以操作快捷键
import pyautogui
# 引入pyperclip模块以操作剪切板
import pyperclip

WxTarget = "文件传输助手"
HotkeyDelay = 2
os.environ['DISPLAY'] = ':0'

def sendWxMessage(msg):
    try:
        # 清空剪切板并将目标写入到剪切板
        pyperclip.copy("")
        pyperclip.copy(WxTarget)
        # 打开微信窗
        pyautogui.hotkey("ctrl", "alt", "w")
        time.sleep(HotkeyDelay)
        # 使用快捷键ctrl+f定位到微信搜索栏
        pyautogui.hotkey("ctrl", "f")
        time.sleep(HotkeyDelay)
        # 使用快捷键ctrl+v将目标粘贴到微信搜索栏，微信将自动搜索
        pyautogui.hotkey("ctrl", "v")
        time.sleep(HotkeyDelay)
        # 按回车键打开搜索出的目标
        pyautogui.press("enter")
        time.sleep(HotkeyDelay)
        # 清空剪切板并将未点检信息写入到剪切板
        pyperclip.copy("")
        pyperclip.copy(msg)
        # 使用快捷键ctrl+v将信息粘贴到微信输入框，按回车发送消息
        pyautogui.hotkey("ctrl", "v")
        time.sleep(HotkeyDelay)
        pyautogui.press("enter")
        # log
        print("发送微信消息")
    except Exception as ex:
        print("发送微信消息出现异常: " + str(ex))
        sys.exit(0)

if __name__ == '__main__':
    sendWxMessage('hello world!')