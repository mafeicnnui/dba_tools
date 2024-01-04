#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/8/29 15:18
# @Author : ma.fei
# @File : test.py
# @Software: PyCharm
import json

str1="""{"BedName":"","Description":"口味\n加浓麻酱卷凉皮\n\n加料3选2（不可重复选）\n卤蛋（1份）\n香肠（1份）\n奥尔良鸡肉（1份）","Floor":"","NearestOrderTime":"","Notification":[{"content":"无","title":"使用规则"}],"RefundPolicy":1,"SuitableGroup":"","View":"","add_bed_fee":0,"appointment":{"need_appointment":false},"can_no_use_date":{"date_list":[],"days_of_week":[],"enable":false,"holidays":[]},"charge_rule":"","image_list":[{"url":"https://pcdn.hopsontone.com/FmuZY7GM61OQTXJD61GnI1B5gJCM"}],"limit_use_rule":{"is_limit_use":false},"poi_list":[{"poi_id":"6601126853623154695"}],"private_room":true,"rec_person_num":1,"rec_person_num_max":1,"show_channel":1,"superimposed_discounts":false,"trade_url":{"app_id":"tt9f7ae9c70045c94d01","params":"{\"comboId\":\"1977\",\"businessId\":83576}","path":"pages/setmeal-detail/setmeal-detail"},"use_date":{"day_duration":30,"use_date_type":2,"use_end_date":"-","use_start_date":"-"},"use_time":{"time_period_list":[{"end_time_is_next_day":false,"use_end_time":"","use_start_time":""}],"use_time_type":1}}"""
str2="""{"EntrySchema":"{\"app_id\":\"tt9f7ae9c70045c94d01\",\"params\":\"{\\\"comboId\\\":\\\"1977\\\",\\\"businessId\\\":83576}\",\"path\":\"pages/setmeal-detail/setmeal-detail\"}","rec_person_num_max":"1","can_no_use_date":"{\"enable\":false}","RefundPolicy":"1","use_time":"{\"use_time_type\":1,\"time_period_list\":[{\"use_start_time\":\"\",\"use_end_time\":\"\",\"end_time_is_next_day\":false}]}","platform_unified_description":"{\"note_type\":1,\"content\":\"如部分菜品因时令或其他不可抗因素导致无法提供，商家会用等价菜品替换，具体事宜请与商家协商。\"}","PostPurchaseDay":"10","Images":"[\"https://sf1-dycdn-tos.pstatp.com/obj/tos-cn-i-lgni0yg6nh/d9857b3b8e0d40c7b55c3f8b1f0c4cc3\"]","appointment":"{\"need_appointment\":false}","Notification":"[{\"title\":\"使用规则\",\"content\":\"无\"}]","superimposed_discounts":"false","SupplierExtIdList":"[\"life_6601126853623154695\"]","trade_url":"{\"app_id\":\"tt9f7ae9c70045c94d01\",\"params\":\"{\\\"comboId\\\":\\\"1977\\\",\\\"businessId\\\":83576}\",\"path\":\"pages/setmeal-detail/setmeal-detail\"}","EntryType":"2","rec_person_num":"1","show_channel":"1","private_room":"true","DateRule":"{\"unavailable_date\":{\"date_list\":[],\"weekday_list\":[],\"not_available_on_holidays\":false}}","image_list":"[{\"url\":\"https://p3-sign.douyinpic.com/obj/tos-cn-i-lgni0yg6nh/d9857b3b8e0d40c7b55c3f8b1f0c4cc3?x-expires=1693296000\\u0026x-signature=amjgkjdC0wxZ6CFnbfh3TjlULRM%3D\\u0026from=709197913\"}]","use_date":"{\"use_date_type\":2,\"day_duration\":30,\"use_start_date\":\"-\",\"use_end_date\":\"-\"}"}"""


#a=json.loads(str1.replace('\n', '\\n'))
b=json.loads(str2.replace('\n', '\\n'))

#print(a,type(a))
print(b,type(b))