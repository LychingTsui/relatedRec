#! /usr/bin/sh

input=/user/tvapk/peichao/TV/movieper/20160420/alltagsarg/*
output="/user/tvapk/cuiliqing/relatedOut"
jarPath="/home/tvapk/run_sh/cuiliqing/"
jarName="RelatedRecModel.jar"
hadoop="/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop"
hdfs="/user/tvapk/cuiliqing/"
localPath="/data/tvapk/cuiliqing/cuiliqing/"

$hadoop fs -rm -r $output
$hadoop fs -rm -r ${hdfs}relatedRecMovie
$hadoop fs -mkdir ${hdfs}relatedRecMovie   ##存储电影的act direct movieTags 文件
# 获取  /user/tvapk/peichao/TV/movieper/20160420/tagsarg/＊ 所有电影中用作features Key的act 文件
$hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.ExtMoviesActorKey $input $output
$hadoop fs -rm -r ${hdfs}relatedRecMovie/act
$hadoop fs -get $output/part-r-00000 ${localPath}
$hadoop fs -rm -r $output

rm ${localPath}act
#在本地路径下为act文件每行添加t1标签，为做缓存文件输入做识别用 t2 t3下同 同样作用
sed 's/^/t1&/g' ${localPath}part-r-00000  > ${localPath}act
actTotal=`cat ${localPath}act | wc -l`   #统计act key的长度，由于锁定 不同的key 对应的id索引
rm ${localPath}part-r-00000

$hadoop fs -put ${localPath}act ${hdfs}relatedRecMovie/
$hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.ExtMovieDirectorkey  $input $output
$hadoop fs -rm -r ${hdfs}relatedRecMovie/direct
$hadoop fs -get $output/part-r-00000 ${localPath}
$hadoop fs -rm -r $output
rm ${localPath}direct
#加t2 标签 作用同t1一样
sed 's/^/t2&/g' ${localPath}part-r-00000  > ${localPath}direct
directTotal=`cat ${localPath}direct | wc -l`
rm ${localPath}part-r-00000
$hadoop fs -put ${localPath}direct  ${hdfs}relatedRecMovie/

# 获取电影的Tags 中用作 feature Key的tags

$hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.ExtMovieAreaKey  $input $output 
$hadoop fs -cp $output/part-r-00000 ${hdfs}countryArea
$hadoop fs -rm -r $output
rm ${localPath}countryArea
$hadoop fs -get  ${hdfs}countryArea ${localPath}
$hadoop fs -rm -r ${hdfs}countryArea 

$hadoop fs -rm -r ${hdfs}relatedRecMovie/area.txt
$hadoop fs -rm -r ${hdfs}relatedRecMovie/country
$hadoop fs -cp ${hdfs}area.txt ${hdfs}relatedRecMovie/  ## 区域
$hadoop fs -cp ${hdfs}country ${hdfs}relatedRecMovie/    # 区域对应固定的idx

areaTot=`$hadoop fs -cat ${hdfs}relatedRecMovie/country | wc -l`

$hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.ExtMovieTagsKey  $input $output
$hadoop fs -rm -r ${hdfs}relatedRecMovie/movieTags
$hadoop fs -get $output/part-r-00000 ${localPath}
$hadoop fs -rm -r $output

rm ${localPath}movieTags
sed 's/^/t3&/g' ${localPath}part-r-00000  > ${localPath}movieTags
movieTags=`cat ${localPath}movieTags | wc -l`
rm ${localPath}part-r-00000
$hadoop fs -put ${localPath}movieTags  ${hdfs}relatedRecMovie/

#$hadoop fs -cp ${hdfs}area.txt ${hdfs}relatedRecMovie/  ## 区域
#$hadoop fs -cp ${hdfs}country ${hdfs}relatedRecMovie/    # 区域对应固定的idx

# features key 拼接后 映射到连续的id索引
directStartId=$[actTotal+1]
areaStartId=$[directStartId+directTotal]
mvTagsStartId=$[areaStartId+areaTot]
mvTagsIdxEnd=$[actTotal+directTotal+areaTot+movieTags]
joinTagsStartId=$[mvTagsIdxEnd+2+1]  ######＋2 上映和打分特征
#secMovActIdStart=$[joinTagsStartId+movieTags+areaTot+directTotal+actTotal]
#secMovDirectIdStart=$[secMovActIdStart+actTotal]
#secMovAreaIdStart=$[secMovDirectIdStart+actTotal+directTotal]
#secMovTagsIdStart=$[secMovAreaIdStart+areaTot]
########### 获取特征id 区间段的起末id索引
rm ${localPath}relatedFeatsIdx.txt
touch  ${localPath}relatedFeatsIdx.txt
echo "direct起始Id: $directStartId" >> ${localPath}relatedFeatsIdx.txt
echo "area起始Id: $areaStartId" >> ${localPath}relatedFeatsIdx.txt
echo "movieTags起始Id: $mvTagsStartId" >> ${localPath}relatedFeatsIdx.txt
echo "joinTags起始Id: $joinTagsStartId" >> ${localPath}relatedFeatsIdx.txt
#echo "the second movie act起始id: $secMovActIdStart" >> ${localPath}relatedFeatsIdx.txt
#echo "the second movie direct起始id: $secMovDirectIdStart" >> ${localPath}relatedFeatsIdx.txt
#echo "the second movie area起始id: $secMovAreaIdStart" >> ${localPath}relatedFeatsIdx.txt
#echo "the second movie movTags起始id: $secMovTagsIdStart" >> ${localPath}relatedFeatsIdx.txt
fileidx=relatedFeatsIdx.txt
############
#一条电影数据 映射成一条movieId：[id:val ...] 格式的数据
#java -cp ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.AreaCountryLib ${localPath}

$hadoop fs -rm -r $output
$hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.relatedRec.ExtMovieItemFeaturesArea  $input $output ${hdfs}relatedRecMovie $directStartId $areaStartId  $mvTagsStartId $mvTagsIdxEnd ######???????????
movieItemFets=movieItemFeatures
$hadoop fs -rm -r $hdfs$movieItemFets
$hadoop fs -cp $output/part-r-00000 $hdfs$movieItemFets
$hadoop fs -rm -r $output
#export $directStartId
export mvTagsStartId 
export joinTagsStartId
export movieItemFets
export fileidx
sh /home/tvapk/run_sh/cuiliqing/relatedRec/extstep2Area.sh
