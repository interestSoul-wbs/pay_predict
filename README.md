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
