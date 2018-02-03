#说明
info1: extstep1 extstep2 是最初版本 第一步电影和第二部电影特征提取的方式相同，交叉tags特征按存在为1的方式处理，
第二部电影的特征id拼接在第一部电影id的后面；

info2: exttest1 exttest2
第一步shell脚本处理过程一样，第二步是在已知样本重复次数下对第二部电影的所有特征＊倍数处理，包括 act，direct，
tags，年份和评分，交叉tags部分仍按存在即为1的处理方式。

info3: ext1.sh ext2noYrDate.sh 在处理拼接的第二部电影时把第二部电影的act direct tags
按*重复次数处理，但第二部电影的上映年份和评分特征不做*重复次数处理

info4: ext1DupGrtOne.sh  ext2DupGrtOne.sh 是把样本中相关点击等于1的样本舍去（视为干扰数据，去燥） 

info5: ext1fullJoinExpkey.sh ext2fullJoinExpkey.sh 全交叉（act，direct，tags，area）包括info4的处理，
除外 还把一些异常的Tag中含的key舍去，不做特征提取。具体看代码，现在的版本训练数据提取采用的就是这种 


relatedRecFullJoin.sh 在model存在指定路径下（模型名为 relatedModell）即可跑出每部电影的相关推进集合
--20171205

xgbRelatedRecAllShow 是xgbBoost模型提取数据的流程 
--20180201
