# pay_predict-工程名称

## 1 python编码规范
### 1.1 文件夹、文件名 及 文件存储
名称小写，可使用"_"来进行分隔；
<br>文件夹，例：</br>
```python
data
userpay
```

文件名，例：
```python
medias_processed.pkl
```

文件存储—— 路径 + 文件名称：
<br>例：</br>
```python
medias_savepath='../../../data/processed/'
df.to_pickle(medias_savepath + 'medias_processed.pkl')
```

### 1.2 函数名
小写，用"_" 分隔；
例：
```python
def get_medias_data():

def get_user_profile():
```

### 1.3 变量
小写，用"_" 分隔
例：
```python
userprofile_train = ""
userprofile_test = ""
```

## 2 scala编码规范
### 2.1 文件夹
名称小写，可使用"_"来进行分隔；
例：
```scala
data
userpay
```

### 2.2 文件名
首字母大写，驼峰命名；
例：
```scala
AuthorVideoList.scala
ChannelPersonalList.scala
```

### 2.3 函数名
首字母小写，驼峰命名；
例：
```scala
def getMediasData():

def getUserProfile():
```

### 2.4 变量
首字母小写，驼峰命名；
例：
```scala
val votesByLang =

val sumByLang = 
```


### 2.5 udf
udf使用方法如下
```scala
val udfRegyxBlank = udf(regyxBlank _)

def regyxBlank(word: String) = {  

    val pattern = "^\\s*$".r  
    val result = pattern replaceAllIn(word, "Na")  

    result
    }
```


## 3 文件系统
文件分**3个**模块：
<br>src —— 代码</br>
<br>data —— 数据</br>
<br>model —— 模型</br>
### 3.1 src
<br>src下存放代码，分为python, scala两个模块，又各自进一步分为train, predict；</br>
<br>train：有关模型训练的所有代码，模型训练-特征工程、模型训练、模型评价-test相关</br>
<br>predict：<u>**独立**</u>地可以对 data/predict里的数据进行特征工程，模型调用模型，预测-predict</br>

### 3.2 data
<br>存放数据，区分为train, predict数据。</br>
<br>train：数据用于模型训练、模型评价，train, test的数据来源；train, test的数据是用train文件夹里的数据自己进行划分的；</br>
<br>predict：该部分数据全部用于“predict”，独立于train，<u>**独立**</u>进行处理！！！predict中的文件为带有后缀"_predict"的文件，这部分用户是独立于train中的用户</br>
<br></br>
<br>train, predict下又细分：</br>
<br>common：共用数据</br>
<br>----raw：原始数据</br>
<br>--------medias：物品数据</br>
<br>--------orders：订单数据</br>
<br>--------plays：播放数据</br>
<br>----processed：处理后的共用数据，直接存放在此，不用再建立文件夹</br>
<br>userpay：用户付费预测专用数据</br>
<br>singlepoint：单点视频预测专用数据</br>


```shell
.
└── pay_predict
    │
    ├── data
    │   │
    │   ├── predict
    │   │   ├── common
    │   │   │   ├── processed
    │   │   │   └── raw
    │   │   │       ├── medias
    │   │   │       ├── orders
    │   │   │       └── plays
    │   │   ├── singlepoint
    │   │   └── userpay
    │   └── train
    │       ├── common
    │       │   ├── processed
    │       │   └── raw
    │       │       ├── medias
    │       │       ├── orders
    │       │       └── plays
    │       ├── singlepoint
    │       └── userpay
    │
    │
    ├── model
    │   ├── singlepoint
    │   └── userpay
    │
    │
    └── src
        ├── python
        │   ├── predict
        │   │   ├── common
        │   │   ├── singlepoint
        │   │   └── userpay
        │   └── train
        │       ├── common
        │       ├── singlepoint
        │       └── userpay
        └── scala
            ├── predict
            │   ├── common
            │   ├── singlepoint
            │   └── userpay
            └── train
                ├── common
                ├── singlepoint
                └── userpay
```

## 4 共用文件
<br>userfile_0601 : '../../../data/train/common/processed/userfile_0601.pkl' </br>
## 5 项目运行
### 5.1 项目运行环境
#### 5.1.1  hdfs+spark
<br>测试的hadoop版本为2.7.1，spark版本为2.4.6，scala的版本为2.11.8，其他的依赖详见pom文件。</br>
#### 5.1.2  python环境和必需的库
<br>python 3.7、tensorflow 2.0.0、deepctr 0.7.5、pyspark 2.4.6、scikit-learn 0.21.1、pandas、numpy</br>
### 5.2 项目文件
#### 5.2.1 jar文件
<br>jar包名为original-pay_predict-1.0-SNAPSHOT.jar，该jar包没有将各种依赖包打包进去，比较小，如果服务器上原有的依赖包满足条件可以执行这个。</br>
<br>pay_predict-1.0-SNAPSHOT.jar,该jar包将项目用到的所有依赖都包括进去，可以直接进行执行。</br>
#### 5.2.2 py文件
<br>套餐付费模型的训练和预测</br>
<br>train_oldusers.py 用于老用户套餐付费模型的训练</br>
<br>train_newusers.py 用于新用户套餐付费模型的训练</br>
<br>predict_oldusers.py 用于老用户套餐付费的预测</br>
<br>predict_newusers.py 用于新用户套餐付费的预测</br>
<br>单点视频付费模型的训练和预测</br>
<br>user_division_train_DeepFM.py 用于用户划分阶段模型的训练。</br>
<br>user_division_predict_DeepFM.py 用于用户划分阶段的预测。</br>
<br>rank_train_DeepFM.py 用于排序阶段模型的训练。</br>
<br>rank_predict_DeeoFM.py 用于排序阶段模型的预测。</br>
### 5.3 项目执行方式
<br>主要通过shell文件进行文件的执行，下面按照执行顺序进行介绍。</br>
<br>shell文件中包含的spark任务的提交参数是根据实验室服务器设置的，可以根据集群的能力进行修改。</br>

1.createfile.sh文件主要用来创建项目所需的文件夹和上传文件,需要执行命令
```shell script
./createfile.sh  medias/ orders/ plays/
```
<br>medias/ 、orders/、plays/分别是存放原始数据（.txt文件）的文件夹，文件夹名不可更改。该命令只需要执行一次。</br>
#### 5.3.1 训练模型
1.trainprofilegenerate.sh在训练阶段执行，主要来处理原始数据，生成某个时间点的用户画像和视频画像，需要执行命令
```shell script
./trainprofilegenerate.sh 2020-06-01 00:00:00
```
2020-06-01 00:00:00是自定的时间点，会以该时间点为准生成到该时间为止的用户画像和视频画像。
<br>2.trainuserpay.sh在训练阶段执行，主要将训练数据中的用户分为新用户和老用户，进行数据处理，然后经过训练得到模型。</br>
```shell script
./trainuserpay.sh 2020-06-01 00:00:00
```
2020-06-01 00:00:00是自定的时间点，会收集该时间点以后两周的用户消费情况作为标签。
<br>3.trainsinglepoint.sh在训练阶段执行，主要进行训练集的生成，用户划分模型的训练，排序模型的训练。</br>
```shell script
./trainsinglepoint.sh 2020-06-01 00:00:00 2020-06-07 00:00:00
```
2020-06-01 00:00:00 2020-06-07 00:00:00是自定的时间点，时间跨度为一周。
<br>4.训练得到的的模型暂时存放到当前目录下。分别为
<br>套餐模型</br>
<br>modeldeepfm_new_spark.h5</br>
<br>model_lgblr_old_spark.h5</br>
<br>单点模型</br>
<br>user_division_DeepFMTemp.h5</br>
<br>rank_DeepFMTemp.h5</br>
#### 5.3.2 将模型用于预测
1.predictprofilegenerate.sh在预测阶段执行，主要来处理原始数据，生成某个时间点的用户画像和视频画像，需要执行命令
```shell script
./predictprofilegenerate.sh 2020-06-07 00:00:00
```
2020-06-07 00:00:00是自定的时间点，会以该时间点为准生成到该时间为止的用户画像和视频画像。
<br>2.predictuserpay.sh在预测阶段执行，主要将用户画像信息输入到模型中去，得到可能购买套餐的用户。</br>
```shell script
./predictuserpay.sh 2020-06-07 00:00:00
```
2020-06-07 00:00:00是自定的时间点，预测结果是从该时间点之后，有哪些用户可能购买套餐。
<br>3.predictsinglepoint.sh在训练阶段执行，主要进行训练集的生成，用户划分模型的训练，排序模型的训练。</br>
```shell script
./predictsinglepoint.sh 2020-06-07 00:00:00 2020-06-14 00:00:00
```
2020-06-07 00:00:00、2020-06-14 00:00:00是自定的时间点，预测结果是该时间段间，有哪些用户可能购买单点视频，可能购买哪些单点视频。
<br>4.预测结果存储在hdfs相应的路径下面。</br>