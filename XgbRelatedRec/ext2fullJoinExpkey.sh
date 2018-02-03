#! /usr/bin/sh

jarName=RelatedRecModel.jar
movieItemFeatPath=$movieItemFets   # 调用俄ext1.sh export 的变量
jarPath=/home/tvapk/run_sh/cuiliqing/
hdfs=/user/tvapk/cuiliqing/
hadoop=/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop
dataSavePath=/data/tvapk/cuiliqing/cuiliqing/
localDir=/data/tvapk/person/temp
tagsIdStart=$mvTagsStartId
joinTagsStart=$joinStartId
$hadoop fs -rm -r ${hdfs}relatedRecNegs
$hadoop fs -mkdir ${hdfs}relatedRecNegs
$hadoop fs -rm -r ${hdfs}relatedRecPos
$hadoop fs -mkdir ${hdfs}relatedRecPos

$hadoop fs -rm -r ${hdfs}relatedInput
$hadoop fs -mkdir ${hdfs}relatedInput

for file in $localDir/*
do  
    str=${file##*/}
    #echo $str
    if [ -f $file -a ${str:4:5} = "click" ]; then
        $hadoop fs -put $file  ${hdfs}relatedRecPos/
       # echo "1--------"
    elif [ -f $file -a ${str:4:4} = "show" ]; then
        $hadoop fs -put $file  ${hdfs}relatedRecNegs/
        #echo "2-----------"
    fi
done
output=/user/tvapk/cuiliqing/relatedOut
$hadoop fs -rm -r $output
$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgModel.ExtRelatedMovieShowPair ${hdfs}relatedRecNegs/ $output 
$hadoop fs -cp $output/part-r-00000 ${hdfs}relatedInput/negs
$hadoop fs -rm -r $output
$hadoop fs -rm -r ${hdfs}relatedRecNegs

$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgModel.ExtRelatedMovieClickPair ${hdfs}relatedRecPos/  $output
$hadoop fs -cp $output/part-r-00000 ${hdfs}relatedInput/pos
$hadoop fs -rm -r $output
$hadoop fs -rm -r ${hdfs}relatedRecPos

$hadoop jar  $jarPath$jarName com.qiguo.tv.movie.relatedRecXgModel.GetRelatedMovieClkRateClkShwTimes ${hdfs}relatedInput/ $output 
$hadoop fs -rm -r ${hdfs}relatedClickRateMvPair
$hadoop fs -cp $output/part-r-00000 ${hdfs}relatedClickRateMvPair 
$hadoop fs -getmerge $output/part-r-00000 /data/tvapk/url/modelStatistics
$hadoop fs -rm -r $output 

$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgModel.ConvertRelatedItem2FeaturesClkShw ${hdfs}relatedClickRateMvPair  $output ${hdfs}$movieItemFeatPath $joinTagsStart 
$hadoop fs -rm -r ${hdfs}relatedRecTrainData
$hadoop fs -cp $output/part-r-00000 ${hdfs}relatedRecTrainData
$hadoop fs -rm -r $output 
#$hadoop fs -rm -r ${hdfs}relatedClickRateMvPair 

$hadoop fs -get ${hdfs}relatedRecTrainData ${dataSavePath}relatedRecData/
$hadoop fs -rm -r ${hdfs}relatedRecTrainData
#rm -rf ${dataSavePath}relatedInput 
#$hadoop fs -get ${hdfs}relatedInput $dataSavePath 
#rm ${dataSavePath}relatedInput/$movieItemFeatPath
#$hadoop fs -get $hdfs$movieItemFeatPath ${dataSavePath}relatedInput/ 
#cat ${dataSavePath}relatedInput/pos >> ${dataSavePath}relatedInput/negs
#uniq -c ${dataSavePath}relatedInput/negs > ${dataSavePath}relatedInput/posneg.txt
#java -cp $jarPath$jarName com.qiguo.tv.movie.relatedRec.ConvertRelatedItem2FeaturesFullJoinwithDateDiff ${dataSavePath}relatedInput/$movieItemFeatPath ${dataSavePath}relatedInput/posneg.txt ${dataSavePath}outdata.txt ${dataSavePath}relatedRecData/$fileidx


#totPos=`cat ${dataSavePath}outdata.txt |awk '$1=="1.0"'| wc -l`
#totNeg=`cat ${dataSavePath}outdata.txt |awk '$1=="0.0"'| wc -l` 
#echo "totPos: $totPos"
#echo "totNeg: $totNeg"
#echo "-----split dataset-----"
#totTrainNeg=$[totNeg*4/5]
#totTrainPos=$[totPos*4/5]
#totTestNeg=$[totNeg/5]
#totTestPos=$[totPos/5]
#trainset=$[totTrainNeg + totTrainPos]
#testset=$[totTestNeg + totTestPos]

#echo "totTrainNeg:$totTrainNeg; totTrainPos: $totTrainPos; totTestNeg: $totTestNeg; totTestPos: $totTestPos"
#echo "trainset: $trainset"
#echo "testset: $testset"

#cat ${dataSavePath}outdata.txt | awk '$1=="1.0"' | head -$totTrainPos  > ${dataSavePath}relatedTrain.txt
#cat ${dataSavePath}outdata.txt | awk '$1=="0.0"' | head -$totTrainNeg >> ${dataSavePath}relatedTrain.txt
#cat ${dataSavePath}outdata.txt | awk '$1=="1.0"' | tail -$totTestPos  > ${dataSavePath}relatedTest.txt
#cat ${dataSavePath}outdata.txt | awk '$1=="0.0"' | tail -$totTestNeg  >> ${dataSavePath}relatedTest.txt 
#echo "-------------------数据完毕-------------------"
#rm ${dataSavePath}outdata.txt 
