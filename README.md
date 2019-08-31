# 微博爬虫
### 参考一些DEMO些的微博自动爬虫  
1. 安装依赖  
pip install -r requirements.txt  
2. 执行脚本  
    user_id_runner.py  爬取用户id，发送到kafka  
    weibo.py  消费kafka的微博id，爬取用户信息和用户微博  
3. 休眠时间根据自己策略设置,作为避免微博屏蔽策略  
4. 有一些已经爬取数据的数据集DEMO

### 参考地址:  
https://github.com/dataabc/weibo-crawler  