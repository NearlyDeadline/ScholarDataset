# Scholar Dataset
## 库
- python > 3.6
- scrapy == 2.4.1
- beautifulsoup4
- pandas
- multidict
- pymysql

## 用法
1. ```git clone```
2. 在ScholarDataset文件夹下建立"config.json"文件，至少包含以下内容：
```
{
    "user": "example_user",
    "password": "example_password",
    "host": "localhost",
    "database": "example_database"
}
```
  
### 从CSV导入数据至MySQL


2. 输入数据应有如下的文件夹结构： 
   
```example_folder/university/professor/example_name/example_disambiguation.csv```
3. 执行如下指令：
   
```python insert_mysql.py --data_dirs example_folder```

如有多个输入文件夹，依此法，将文件夹名称附加在```example_folder```之后即可（记得加空格）

### 使用Web of Science爬虫
1. 确保IP拥有网站访问权限
2. 执行如下指令：

```python update_mysql_wos.py```

