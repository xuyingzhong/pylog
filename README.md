# 参考 filebeat做的python脚本，分为两个部分，log读取到redis，redis到es。
# log读取到redis，默认合并了行，然后可以挑选必须，和剔除不要的，他们是相交的，不是相斥的，有必须的，哪怕在剔除中，也会读取。
# redis到es采用批量插入，只要redis有，每次500条，如果不够500条，但是redis数据没有了，也及时插入。
