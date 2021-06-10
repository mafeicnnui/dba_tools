{
	backgroundColor: '#FFFFFF',  //设置背景色
	visualMap: [{
		show: false,
		type: 'continuous',
		seriesIndex: 0,
		min: 0,
		max: 1000
	}],
	title: [{
		 left: 'center',
		 text: '活动连接:15:59~17:00',
		 textStyle:{
			color:'#7b7b76',
			fontSize:12,
			fontStyle:'normal',
			fontFamily:'sans-serif'
		 }
	}],
	animation:false,  //关闭动画
	xAxis: [{
                type: 'category',
                boundaryGap: false,
                interval:60,
		data: ["04:06","04:16","04:26","04:36","04:46","04:56"],
		name:'Time',
		axisLine:{
		           lineStyle:{
			     color:"black",
			     width:1,
		            }
		        }
	}],
	yAxis: [{
		name:'Active',
                type: 'value',
		splitLine: {show: false},
		axisLine:{
                        show:true,
			lineStyle:{
				color:"'#ccc",
			}
		}
	}],
        grid: [
                {x:30,y:40,x2:50,y2:50},
        ],
	series: [{
		type: 'line',
		symbol: "none",
		showSymbol: true,
                areaStyle: {normal: {
                                     color: new echarts.graphic.LinearGradient(
                                     0, 0, 0, 1,
                                     [
                                        {offset: 0,   color: '#348dff'},
                                        {offset: 0.5, color: '#348dff'},
                                        {offset: 1,   color: '#3c70dd'}
                                     ]
                                    )
                            }},  
                itemStyle : {
                                normal : {
                                color:'#348dff',
                                borderColor: "#348dff",
                                lineStyle:{
                                    color:'#348dff',
                                    width:1,
                                }
                              }
                            },
                data: [2,5,1,2,4,2]
	}]
}
