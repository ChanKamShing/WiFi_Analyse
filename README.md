# WiFi_Analyse
<p>基于wifi路由收集的数据，对商铺客流信息进行分析</p>
<p>&nbsp;&nbsp;&nbsp;&nbsp;(详细要求参考：http://www.cnsoftbei.com/bencandy.php?fid=148&id=1515)</p>
<h1>#项目介绍：</h1>
<ul>1、基于wifi设备，通过其探针设备采集可检测范围内的手机MAC地址、与探针距离、时间、地理位置等信息；</ul>
<ul>2、探针收集的数据定时向服务器传送，并保存；</ul>
<ul>3、利用大数据技术对数据进行客户流量信息等指标进行分析。</ul>
<p>数据被收集到服务端，经过清洗后的格式为：</p>
<p>{"tanzhen_id":"00aabbce","mac":"a4:56:02:61:7f:1a","time":"1492913100","rssi":"95","range":"1"}</p>
<p>&nbsp;&nbsp;&nbsp;&nbsp;事实上，客流信息包括：客流量、入店量、入店率、来访周期、新老顾客、顾客活跃度、驻店时长、跳出率、深访率等。</p>
<p>那么可以进一步处理，得到一张中间表，数据格式为：</p>
<p>{"mac":"a4:56:02:61:7f:1a","in_time":"xxxxxx","out_time":"xxxxxx","stay_time":"xxxxxx"}</p>
<p>&nbsp;&nbsp;&nbsp;&nbsp;所有指标都可以通过中间表进行简单sql操作可以得到。</p>
